/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! Conversion from a parquet footer (parquet2 `FileMetaData` or raw thrift row
//! groups) to the `_pm` binary representation.
//!
//! Two entry shapes are supported:
//!
//! - Callers that already constructed thrift row groups in memory (the
//!   parquet write path is the canonical example) call
//!   [`build_row_group_block`] per row group with pre-built
//!   [`ParquetMetaColumnInfo`]s.
//! - Callers that have a parsed parquet2 [`FileMetaData`] call
//!   [`convert_from_parquet`], which extracts the column descriptors from the
//!   schema.
//!
//! Both share the same per-chunk transformation; the only differences are how
//! column descriptors are sourced and how bloom-filter bitsets are resolved
//! (see [`BloomFilterSource`]).

use parquet2::metadata::FileMetaData;
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType};
use parquet2::thrift_format::{ColumnMetaData, RowGroup};
use qdb_core::col_type::ColumnTypeTag;

use crate::column_chunk::ColumnChunkRaw;
use crate::error::{ParquetMetaErrorKind, ParquetMetaResult};
use crate::infer::infer_column_type;
use crate::parquet_meta_err;
use crate::qdb_meta::{QdbMeta, QdbMetaColFormat};
use crate::row_group::RowGroupBlockBuilder;
use crate::types::{encode_stat_sizes, Codec, ColumnFlags, EncodingMask, FieldRepetition, SeqTxn, StatFlags};
use crate::writer::ParquetMetaWriter;

/// Column descriptor needed to build a `_pm` header. Callers build this from a
/// parquet2 `ColumnDescriptor` (+ optional `QdbMeta`) or from their own column
/// state.
pub struct ParquetMetaColumnInfo<'a> {
    pub name: &'a str,
    pub col_type_code: i32,
    pub id: i32,
    pub flags: ColumnFlags,
    pub fixed_byte_len: i32,
    pub physical_type: u8,
    pub max_rep_level: u8,
    pub max_def_level: u8,
}

/// Resolves parquet bloom-filter bitsets while converting row groups.
///
/// Parquet footers store bloom filters as `(offset, length)` byte ranges in
/// the parquet file. Different callers have different ways to resolve those:
///
/// - Writers that capture bitsets in memory while writing use a custom
///   `BloomFilterSource` keyed by `(row_group, column)`.
/// - Callers that already have the parquet file's bytes available — mmap or
///   buffer — use [`SliceBloomFilterSource`].
/// - Callers that do not need bloom inlining pass [`NoBloomFilterSource`].
pub trait BloomFilterSource {
    fn bitset<'a>(
        &'a self,
        row_group: usize,
        column: usize,
        meta: &ColumnMetaData,
    ) -> ParquetMetaResult<Option<&'a [u8]>>;
}

/// Bloom source that always returns `None`. Used when the caller does not want
/// bloom bitsets inlined into `_pm`.
pub struct NoBloomFilterSource;

impl BloomFilterSource for NoBloomFilterSource {
    fn bitset<'a>(
        &'a self,
        _row_group: usize,
        _column: usize,
        _meta: &ColumnMetaData,
    ) -> ParquetMetaResult<Option<&'a [u8]>> {
        Ok(None)
    }
}

/// Bloom source backed by a contiguous slice of the parquet file's bytes,
/// using `meta.bloom_filter_offset` to locate the bitset. Suitable for any
/// caller that already has the parquet file mmapped or loaded.
pub struct SliceBloomFilterSource<'a> {
    data: &'a [u8],
}

impl<'a> SliceBloomFilterSource<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl BloomFilterSource for SliceBloomFilterSource<'_> {
    fn bitset<'a>(
        &'a self,
        _row_group: usize,
        _column: usize,
        meta: &ColumnMetaData,
    ) -> ParquetMetaResult<Option<&'a [u8]>> {
        let Some(offset) = meta.bloom_filter_offset else {
            return Ok(None);
        };
        if offset <= 0 {
            return Ok(None);
        }
        let bitset = parquet2::bloom_filter::read_from_slice_at_offset(offset as u64, self.data)
            .map_err(|err| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Conversion,
                    "could not read parquet bloom filter at offset {offset}: {err}"
                )
            })?;
        if bitset.is_empty() {
            Ok(None)
        } else {
            Ok(Some(bitset))
        }
    }
}

/// Closure invoked by [`convert_from_parquet`] when a row group's designated
/// timestamp column lacks inline min/max statistics. The converter calls it
/// with `(rg_idx, 0, 1)` for min and `(rg_idx, num_values - 1, num_values)`
/// for max.
pub type TsStatsBackfill<'a> = dyn Fn(usize, usize, usize) -> ParquetMetaResult<i64> + 'a;

/// Maps a parquet2 `PhysicalType` enum to its ordinal `u8` encoding used in
/// the `_pm` header.
pub fn physical_type_to_u8(pt: PhysicalType) -> u8 {
    match pt {
        PhysicalType::Boolean => 0,
        PhysicalType::Int32 => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::Int96 => 3,
        PhysicalType::Float => 4,
        PhysicalType::Double => 5,
        PhysicalType::ByteArray => 6,
        PhysicalType::FixedLenByteArray(_) => 7,
    }
}

/// Sorting column extracted from a parquet footer. `column_idx` is the leaf
/// column index in `FileMetaData.schema_descr.columns()`.
pub struct SortingCol {
    pub column_idx: i32,
    pub descending: bool,
}

/// Extracts the sorting columns shared by all row groups. Returns an empty
/// vector when the footer carries no sorting columns. Returns an error if the
/// sorting columns differ between row groups, since `_pm` stores a single
/// per-file list.
pub fn extract_sorting_columns(file_metadata: &FileMetaData) -> ParquetMetaResult<Vec<SortingCol>> {
    let mut result: Option<Vec<SortingCol>> = None;

    for (rg_idx, rg) in file_metadata.row_groups.iter().enumerate() {
        let current = match rg.sorting_columns() {
            Some(cols) => cols
                .iter()
                .map(|sc| SortingCol {
                    column_idx: sc.column_idx,
                    descending: sc.descending,
                })
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };

        if let Some(ref prev) = result {
            if prev.len() != current.len() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::SchemaMismatch,
                    "sorting columns differ between row groups: rg 0 has {} but rg {} has {}",
                    prev.len(),
                    rg_idx,
                    current.len()
                ));
            }
            for (i, (p, c)) in prev.iter().zip(current.iter()).enumerate() {
                if p.column_idx != c.column_idx || p.descending != c.descending {
                    return Err(parquet_meta_err!(
                        ParquetMetaErrorKind::SchemaMismatch,
                        "sorting column {} differs between row groups 0 and {}",
                        i,
                        rg_idx
                    ));
                }
            }
        } else {
            result = Some(current);
        }
    }

    Ok(result.unwrap_or_default())
}

/// Detects the designated timestamp column from QdbMeta (preferred) or by
/// matching the parquet sorting columns + logical type heuristic.
pub fn detect_designated_timestamp(
    file_metadata: &FileMetaData,
    qdb_meta: Option<&QdbMeta>,
    sorting_cols: &[SortingCol],
) -> i32 {
    if let Some(meta) = qdb_meta {
        for (i, col) in meta.schema.iter().enumerate() {
            if col.column_type.is_designated() {
                return i as i32;
            }
        }
    }

    if let Some(first_sc) = sorting_cols.first() {
        if !first_sc.descending {
            let col_idx = first_sc.column_idx as usize;
            if let Some(col_desc) = file_metadata.schema_descr.columns().get(col_idx) {
                let info = &col_desc.descriptor.primitive_type.field_info;
                if info.repetition == parquet2::schema::Repetition::Required {
                    let is_timestamp = if let Some(meta) = qdb_meta {
                        meta.schema
                            .get(col_idx)
                            .is_some_and(|cm| cm.column_type.tag() == ColumnTypeTag::Timestamp)
                    } else {
                        matches!(
                            col_desc.descriptor.primitive_type.logical_type,
                            Some(PrimitiveLogicalType::Timestamp { .. })
                        )
                    };
                    if is_timestamp {
                        return col_idx as i32;
                    }
                }
            }
        }
    }

    -1
}

/// Verifies that no column chunk references an external file path. `_pm`
/// targets self-contained parquet files; external references would invalidate
/// the byte-range layout assumptions.
pub fn validate_file_paths(file_metadata: &FileMetaData) -> ParquetMetaResult<()> {
    for (rg_idx, rg) in file_metadata.row_groups.iter().enumerate() {
        for (col_idx, col) in rg.columns().iter().enumerate() {
            if col.file_path().is_some() {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::Conversion,
                    "column chunk at rg={}, col={} references an external file_path (not supported)",
                    rg_idx,
                    col_idx
                ));
            }
        }
    }
    Ok(())
}

/// Converts a parquet file's metadata into a `_pm` binary representation.
///
/// - `file_metadata`: parsed parquet footer (typically from
///   `parquet2::read::read_metadata_with_size`).
/// - `qdb_meta`: optional QuestDB-specific metadata extracted from the parquet
///   footer's `"questdb"` key-value entry. When absent, column types are
///   inferred via [`infer_column_type`].
/// - `parquet_footer_offset`, `parquet_footer_length`: position of the parquet
///   footer inside the parquet file (recorded in `_pm` so readers can locate
///   it without re-parsing).
/// - `bloom_source`: resolves parquet-level bloom filter bitsets so the
///   converter can inline them into `_pm`. Pass [`NoBloomFilterSource`] to
///   skip bloom inlining; pass [`SliceBloomFilterSource`] when the parquet
///   bytes are available.
/// - `ts_stats_backfill`: optional decode callback the converter calls when a
///   row group's designated timestamp column lacks inline min/max stats.
#[allow(clippy::too_many_arguments)]
pub fn convert_from_parquet(
    file_metadata: &FileMetaData,
    qdb_meta: Option<&QdbMeta>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_source: &dyn BloomFilterSource,
    ts_stats_backfill: Option<&TsStatsBackfill<'_>>,
) -> ParquetMetaResult<(Vec<u8>, u64)> {
    let columns = file_metadata.schema_descr.columns();
    let col_count = columns.len();

    if let Some(meta) = qdb_meta {
        if meta.schema.len() != col_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::SchemaMismatch,
                "QdbMeta schema has {} columns but parquet has {}",
                meta.schema.len(),
                col_count
            ));
        }
    }

    validate_file_paths(file_metadata)?;
    let sorting_cols = extract_sorting_columns(file_metadata)?;
    let designated_ts = detect_designated_timestamp(file_metadata, qdb_meta, &sorting_cols);

    let mut writer = ParquetMetaWriter::new();
    writer.designated_timestamp(designated_ts);
    writer.parquet_footer(parquet_footer_offset, parquet_footer_length);
    if let Some(meta) = qdb_meta {
        writer.squash_tracker(meta.squash_tracker);
        writer.seq_txn(SeqTxn::new(meta.seq_txn));
    }

    for sc in &sorting_cols {
        writer.add_sorting_column(sc.column_idx as u32);
    }

    for (i, col_desc) in columns.iter().enumerate() {
        let field_info = col_desc.base_type.get_field_info();
        let name = &field_info.name;
        let id = field_info.id.unwrap_or(-1);

        let col_type_code = if let Some(meta) = qdb_meta {
            meta.schema[i].column_type.code()
        } else {
            infer_column_type(col_desc).map(|t| t.code()).unwrap_or(-1)
        };

        let mut flags = ColumnFlags::new()
            .with_repetition(FieldRepetition::from(field_info.repetition));

        if let Some(meta) = qdb_meta {
            let col_meta = &meta.schema[i];
            if col_meta.format == Some(QdbMetaColFormat::LocalKeyIsGlobal) {
                flags = flags.with_local_key_is_global();
            }
            if col_meta.ascii == Some(true) {
                flags = flags.with_ascii();
            }
        }

        if let Some(sc) = sorting_cols.iter().find(|sc| sc.column_idx == i as i32) {
            if sc.descending {
                flags = flags.with_descending();
            }
        }

        let phys_type = col_desc.descriptor.primitive_type.physical_type;
        let fixed_byte_len = match phys_type {
            PhysicalType::FixedLenByteArray(len) => len as i32,
            _ => 0,
        };
        let max_rep_level: u8 = col_desc.descriptor.max_rep_level.try_into().map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "max_rep_level {} does not fit in u8",
                col_desc.descriptor.max_rep_level
            )
        })?;
        let max_def_level: u8 = col_desc.descriptor.max_def_level.try_into().map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "max_def_level {} does not fit in u8",
                col_desc.descriptor.max_def_level
            )
        })?;
        writer.add_column(
            name,
            id,
            col_type_code,
            flags,
            fixed_byte_len,
            physical_type_to_u8(phys_type),
            max_rep_level,
            max_def_level,
        );
    }

    for (rg_idx, rg) in file_metadata.row_groups.iter().enumerate() {
        let rg_columns = rg.columns();
        if rg_columns.len() != col_count {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::SchemaMismatch,
                "row group has {} columns but schema has {}",
                rg_columns.len(),
                col_count
            ));
        }

        let mut rg_builder = RowGroupBlockBuilder::new(col_count as u32);
        rg_builder.set_num_rows(rg.num_rows().max(0) as u64);

        for (col_idx, col_chunk) in rg_columns.iter().enumerate() {
            let meta = col_chunk.metadata();
            let mut chunk = build_column_chunk(meta)?;

            // Backfill inline min/max stats for the designated timestamp
            // column when the source parquet lacks them. Without this the
            // `_pm` would force readers onto the decode fallback path,
            // defeating the "`_pm` is authoritative" invariant.
            if col_idx as i32 == designated_ts && chunk.raw.num_values > 0 {
                let col_type_tag = qdb_meta
                    .and_then(|m| {
                        let code = m.schema[col_idx].column_type.tag() as u8;
                        ColumnTypeTag::try_from(code).ok()
                    })
                    .or_else(|| infer_column_type(&columns[col_idx]).map(|ct| ct.tag()));
                if col_type_tag == Some(ColumnTypeTag::Timestamp) {
                    if let Some(backfill) = ts_stats_backfill {
                        let stat_flags = StatFlags(chunk.raw.stat_flags);
                        let has_min_inlined =
                            stat_flags.has_min_stat() && stat_flags.is_min_inlined();
                        let has_max_inlined =
                            stat_flags.has_max_stat() && stat_flags.is_max_inlined();
                        if !has_min_inlined || !has_max_inlined {
                            let num_values = chunk.raw.num_values as usize;
                            let min_ts = backfill(rg_idx, 0, 1)?;
                            let max_ts =
                                backfill(rg_idx, num_values - 1, num_values)?;
                            chunk.raw.min_stat = min_ts as u64;
                            chunk.raw.max_stat = max_ts as u64;
                            chunk.raw.stat_flags =
                                stat_flags.with_min(true, true).with_max(true, true).0;
                            chunk.raw.stat_sizes = encode_stat_sizes(8, 8);
                            chunk.ool_min = None;
                            chunk.ool_max = None;
                        }
                    }
                }
            }

            fill_column_chunk(&mut rg_builder, col_idx, rg_idx, &chunk, meta, bloom_source)?;
        }

        writer.add_row_group(rg_builder);
    }

    writer.finish()
}

/// Generates a complete `_pm` file from pre-built column descriptors and raw
/// thrift row groups. Used by the parquet write path which never materializes
/// a parquet2 [`FileMetaData`].
#[allow(clippy::too_many_arguments)]
pub fn generate_parquet_metadata(
    columns: &[ParquetMetaColumnInfo<'_>],
    thrift_row_groups: &[RowGroup],
    designated_timestamp: i32,
    sorting_columns: &[u32],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    unused_bytes: u64,
    squash_tracker: i64,
    seq_txn: SeqTxn,
    bloom_source: &dyn BloomFilterSource,
) -> ParquetMetaResult<(Vec<u8>, u64)> {
    let mut writer = ParquetMetaWriter::new();
    writer.designated_timestamp(designated_timestamp);
    writer.parquet_footer(parquet_footer_offset, parquet_footer_length);
    writer.unused_bytes(unused_bytes);
    writer.squash_tracker(squash_tracker);
    writer.seq_txn(seq_txn);

    for &sc_idx in sorting_columns {
        writer.add_sorting_column(sc_idx);
    }

    for col in columns {
        writer.add_column(
            col.name,
            col.id,
            col.col_type_code,
            col.flags,
            col.fixed_byte_len,
            col.physical_type,
            col.max_rep_level,
            col.max_def_level,
        );
    }

    for (rg_idx, thrift_rg) in thrift_row_groups.iter().enumerate() {
        let block = build_row_group_block(thrift_rg, rg_idx, bloom_source)?;
        writer.add_row_group(block);
    }

    writer.finish()
}

/// Builds a row-group block from a raw thrift `RowGroup`, including inlining
/// any bloom-filter bitsets the `bloom_source` resolves.
pub fn build_row_group_block(
    rg: &RowGroup,
    rg_idx: usize,
    bloom_source: &dyn BloomFilterSource,
) -> ParquetMetaResult<RowGroupBlockBuilder> {
    let col_count = rg.columns.len();
    let mut builder = RowGroupBlockBuilder::new(col_count as u32);
    builder.set_num_rows(rg.num_rows.max(0) as u64);

    for (col_idx, col_chunk) in rg.columns.iter().enumerate() {
        let meta = col_chunk.meta_data.as_ref().ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "column chunk at rg={}, col={} has no metadata",
                rg_idx,
                col_idx
            )
        })?;
        let chunk = build_column_chunk(meta)?;
        fill_column_chunk(&mut builder, col_idx, rg_idx, &chunk, meta, bloom_source)?;
    }

    Ok(builder)
}

pub(crate) struct BuiltChunk {
    pub raw: ColumnChunkRaw,
    pub ool_min: Option<Vec<u8>>,
    pub ool_max: Option<Vec<u8>>,
}

/// Transforms a parquet thrift column-chunk metadata into the raw `_pm`
/// representation and any out-of-line stats bytes. Bloom filters are NOT read
/// here; the post-chunk [`fill_column_chunk`] step resolves them through a
/// [`BloomFilterSource`].
pub(crate) fn build_column_chunk(meta: &ColumnMetaData) -> ParquetMetaResult<BuiltChunk> {
    let byte_range_start = meta
        .dictionary_page_offset
        .unwrap_or(meta.data_page_offset)
        .max(0) as u64;
    let total_compressed = meta.total_compressed_size.max(0) as u64;

    let codec = parquet2::compression::Compression::try_from(meta.codec)
        .map(Codec::from)
        .map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "unsupported compression codec: {:?}",
                meta.codec
            )
        })?;

    let p2_encodings: Vec<parquet2::encoding::Encoding> = meta
        .encodings
        .iter()
        .filter_map(|e| parquet2::encoding::Encoding::try_from(*e).ok())
        .collect();
    let encodings = EncodingMask::from(p2_encodings.as_slice());

    let mut raw = ColumnChunkRaw::zeroed();
    raw.codec = codec as u8;
    raw.encodings = encodings.0;
    raw.num_values = meta.num_values.max(0) as u64;
    raw.byte_range_start = byte_range_start;
    raw.total_compressed = total_compressed;

    let (ool_min, ool_max) = apply_thrift_stats(&mut raw, meta.statistics.as_ref());

    Ok(BuiltChunk { raw, ool_min, ool_max })
}

/// Inserts a built column chunk into a row-group builder, appends any
/// out-of-line min/max stats, and inlines a bloom-filter bitset if the
/// `bloom_source` resolves one. Empty bitsets are skipped.
fn fill_column_chunk(
    builder: &mut RowGroupBlockBuilder,
    col_idx: usize,
    rg_idx: usize,
    chunk: &BuiltChunk,
    meta: &ColumnMetaData,
    bloom_source: &dyn BloomFilterSource,
) -> ParquetMetaResult<()> {
    builder.set_column_chunk(col_idx, chunk.raw)?;
    if let Some(ref min_bytes) = chunk.ool_min {
        builder.add_out_of_line_stat(col_idx, true, min_bytes)?;
    }
    if let Some(ref max_bytes) = chunk.ool_max {
        builder.add_out_of_line_stat(col_idx, false, max_bytes)?;
    }
    let has_bloom = matches!(
        (meta.bloom_filter_offset, meta.bloom_filter_length),
        (Some(off), Some(len)) if off > 0 && len > 0
    );
    if has_bloom {
        if let Some(bitset) = bloom_source.bitset(rg_idx, col_idx, meta)? {
            if !bitset.is_empty() {
                builder.add_bloom_filter(col_idx, bitset)?;
            }
        }
    }
    Ok(())
}

/// Reads min/max/null/distinct counts from parquet's thrift statistics into
/// `raw` and returns any out-of-line bytes.
///
/// Inline vs OOL is gated purely by stat byte width (1..=8 bytes inline,
/// longer goes OOL): the QuestDB column type does not constrain placement,
/// because the read path (`can_skip_row_group`, `find_row_group_by_timestamp`)
/// already interprets the slot at parquet physical width and applies any
/// parquet-aware overlay (e.g., `is_date * MILLIS_PER_DAY`) on its own.
fn apply_thrift_stats(
    raw: &mut ColumnChunkRaw,
    stats: Option<&parquet2::thrift_format::Statistics>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let Some(stats) = stats else {
        return (None, None);
    };

    let mut stat_flags = StatFlags::new();
    let mut ool_min: Option<Vec<u8>> = None;
    let mut ool_max: Option<Vec<u8>> = None;

    if let Some(nc) = stats.null_count {
        stat_flags = stat_flags.with_null_count();
        raw.null_count = nc.max(0) as u64;
    }
    if let Some(dc) = stats.distinct_count {
        stat_flags = stat_flags.with_distinct_count();
        raw.distinct_count = dc.max(0) as u64;
    }

    if let Some(min_val) = stats.min_value.as_deref() {
        if !min_val.is_empty() {
            if min_val.len() <= 8 {
                stat_flags = stat_flags.with_min(true, true);
                let mut buf = [0u8; 8];
                buf[..min_val.len()].copy_from_slice(min_val);
                raw.min_stat = u64::from_le_bytes(buf);
            } else {
                stat_flags = stat_flags.with_min(false, true);
                ool_min = Some(min_val.to_vec());
            }
        }
    }

    if let Some(max_val) = stats.max_value.as_deref() {
        if !max_val.is_empty() {
            if max_val.len() <= 8 {
                stat_flags = stat_flags.with_max(true, true);
                let mut buf = [0u8; 8];
                buf[..max_val.len()].copy_from_slice(max_val);
                raw.max_stat = u64::from_le_bytes(buf);
            } else {
                stat_flags = stat_flags.with_max(false, true);
                ool_max = Some(max_val.to_vec());
            }
        }
    }

    let min_size = if stat_flags.is_min_inlined() {
        stats.min_value.as_ref().map(|v| v.len() as u8).unwrap_or(0)
    } else {
        0
    };
    let max_size = if stat_flags.is_max_inlined() {
        stats.max_value.as_ref().map(|v| v.len() as u8).unwrap_or(0)
    } else {
        0
    };
    if min_size > 0 || max_size > 0 {
        raw.stat_sizes = encode_stat_sizes(min_size, max_size);
    }
    raw.stat_flags = stat_flags.0;

    (ool_min, ool_max)
}

