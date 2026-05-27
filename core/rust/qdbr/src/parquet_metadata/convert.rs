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

//! Conversion from parquet2 `FileMetaData` (+ optional `QdbMeta`) to `_pm` format.

use crate::parquet::error::{parquet_meta_err, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaColFormat};
use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
use crate::parquet_metadata::error::ParquetMetaErrorKind;
use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
use crate::parquet_metadata::types::{
    encode_stat_sizes, Codec, ColumnFlags, EncodingMask, FieldRepetition, StatFlags,
};
use crate::parquet_metadata::writer::ParquetMetaWriter;
use parquet2::metadata::FileMetaData;
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType};
use qdb_core::col_type::ColumnTypeTag;

/// Maps a parquet2 `PhysicalType` enum to its ordinal `u8` encoding.
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

/// Decodes a timestamp for row_group_index, row_lo, row_hi. The converter
/// invokes this to backfill missing min and max statistics on the designated
/// timestamp column.
pub type TsStatsBackfill<'a> = dyn Fn(usize, usize, usize) -> ParquetResult<i64> + 'a;

/// Converts a parquet file's metadata into a `_pm` binary representation.
///
/// # Arguments
/// - `file_metadata` - Parquet file metadata from `read_metadata_with_size()`.
/// - `qdb_meta` - Optional QuestDB-specific metadata (from the parquet footer's
///   `"questdb"` key-value pair). If `None`, column types are inferred from the
///   parquet schema and tops default to 0.
/// - `parquet_footer_offset` - Byte offset of the parquet footer in the parquet file.
/// - `parquet_footer_length` - Length of the parquet footer in bytes.
/// - `ts_stats_backfill` - Optional callback used when a row group's designated
///   timestamp column lacks inline min/max stats. When provided, the converter
///   invokes it with `(rg_idx, 0, 1)` for min and `(rg_idx, num_values - 1,
///   num_values)` for max, then writes the results as inline stats.
///
/// # Errors
/// - If any column chunk references an external `file_path` (not supported).
/// - If sorting columns differ between row groups.
/// - If `qdb_meta` is present but its schema length doesn't match the parquet column count.
pub fn convert_from_parquet(
    file_metadata: &FileMetaData,
    qdb_meta: Option<&QdbMeta>,
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    ts_stats_backfill: Option<&TsStatsBackfill<'_>>,
) -> ParquetResult<(Vec<u8>, u64)> {
    let columns = file_metadata.schema_descr.columns();
    let col_count = columns.len();

    // Validate QdbMeta schema length matches.
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

    // Validate no file_path references and extract/validate sorting columns.
    validate_file_paths(file_metadata)?;
    let sorting_cols = extract_sorting_columns(file_metadata)?;

    // Detect designated timestamp.
    let designated_ts = detect_designated_timestamp(file_metadata, qdb_meta, &sorting_cols);

    // Build the writer.
    let mut writer = ParquetMetaWriter::new();
    writer.designated_timestamp(designated_ts);
    writer.parquet_footer(parquet_footer_offset, parquet_footer_length);
    if let Some(meta) = qdb_meta {
        writer.squash_tracker(meta.squash_tracker);
    }

    // Add sorting columns.
    for sc in &sorting_cols {
        writer.add_sorting_column(sc.column_idx as u32);
    }

    // Add column descriptors.
    for (i, col_desc) in columns.iter().enumerate() {
        let field_info = col_desc.base_type.get_field_info();
        let name = &field_info.name;
        let id = field_info.id.unwrap_or(-1);

        let col_type_code = if let Some(meta) = qdb_meta {
            let col_meta = &meta.schema[i];
            col_meta.column_type.code()
        } else {
            // Without QdbMeta, infer the QDB type from the parquet schema.
            let inferred = crate::parquet_read::meta::infer_column_type(col_desc);
            inferred.map(|t| t.code()).unwrap_or(-1)
        };

        let mut flags = ColumnFlags::new();

        // Set repetition from parquet schema.
        let repetition = FieldRepetition::from(field_info.repetition);
        flags = flags.with_repetition(repetition);

        // Set QdbMeta-derived flags.
        if let Some(meta) = qdb_meta {
            let col_meta = &meta.schema[i];
            if col_meta.format == Some(QdbMetaColFormat::LocalKeyIsGlobal) {
                flags = flags.with_local_key_is_global();
            }
            if col_meta.ascii == Some(true) {
                flags = flags.with_ascii();
            }
        }

        // Set descending from sorting columns.
        if let Some(sc) = sorting_cols.iter().find(|sc| sc.column_idx == i as i32) {
            if sc.descending {
                flags = flags.with_descending();
            }
        }

        let phys_type = col_desc.descriptor.primitive_type.physical_type;
        let physical_type = physical_type_to_u8(phys_type);
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
            physical_type,
            max_rep_level,
            max_def_level,
        );
    }

    // Add row groups.
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
            let col_type_tag = qdb_meta
                .and_then(|m| {
                    let code = m.schema[col_idx].column_type.tag() as u8;
                    ColumnTypeTag::try_from(code).ok()
                })
                .or_else(|| {
                    crate::parquet_read::meta::infer_column_type(&columns[col_idx])
                        .map(|ct| ct.tag())
                });

            let mut chunk = build_column_chunk(col_chunk)?;

            // Backfill inline min/max stats for the designated timestamp column
            // when the source parquet lacks them. Without this the `_pm` would
            // force readers onto the decode fallback path, defeating the
            // "`_pm` is authoritative" invariant. The closure is only invoked
            // when the chunk has non-zero values and at least one of the
            // min/max stats is missing or not inlined.
            if col_idx as i32 == designated_ts
                && col_type_tag == Some(ColumnTypeTag::Timestamp)
                && chunk.raw.num_values > 0
            {
                if let Some(backfill) = ts_stats_backfill {
                    let stat_flags = StatFlags(chunk.raw.stat_flags);
                    let has_min_inlined = stat_flags.has_min_stat() && stat_flags.is_min_inlined();
                    let has_max_inlined = stat_flags.has_max_stat() && stat_flags.is_max_inlined();
                    if !has_min_inlined || !has_max_inlined {
                        let num_values = chunk.raw.num_values as usize;
                        let min_ts = backfill(rg_idx, 0, 1)?;
                        let max_ts = backfill(rg_idx, num_values - 1, num_values)?;
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

            rg_builder.set_column_chunk(col_idx, chunk.raw)?;

            // Add out-of-line stats if any.
            if let Some(ref min_bytes) = chunk.ool_min {
                rg_builder.add_out_of_line_stat(col_idx, true, min_bytes)?;
            }
            if let Some(ref max_bytes) = chunk.ool_max {
                rg_builder.add_out_of_line_stat(col_idx, false, max_bytes)?;
            }
        }

        writer.add_row_group(rg_builder);
    }

    Ok(writer.finish()?)
}

struct BuiltChunk {
    raw: ColumnChunkRaw,
    ool_min: Option<Vec<u8>>,
    ool_max: Option<Vec<u8>>,
    /// Bloom filter location in the parquet file (offset, length).
    bloom_filter_parquet: Option<(u64, u32)>,
}

fn build_column_chunk(
    col_chunk: &parquet2::metadata::ColumnChunkMetaData,
) -> ParquetResult<BuiltChunk> {
    let (byte_range_start, total_compressed) = col_chunk.byte_range();
    let codec = Codec::from(col_chunk.compression());
    // column_encoding() returns parquet2::thrift_format's Encoding; convert to parquet2's.
    let p2_encodings: Vec<parquet2::encoding::Encoding> = col_chunk
        .column_encoding()
        .iter()
        .filter_map(|e| parquet2::encoding::Encoding::try_from(*e).ok())
        .collect();
    let encodings = EncodingMask::from(p2_encodings.as_slice());

    let m = col_chunk.metadata();
    let bloom_filter_parquet = match (m.bloom_filter_offset, m.bloom_filter_length) {
        (Some(off), Some(len)) if off > 0 && len > 0 => Some((off.max(0) as u64, len as u32)),
        _ => None,
    };

    let mut raw = ColumnChunkRaw::zeroed();
    raw.codec = codec as u8;
    raw.encodings = encodings.0;
    raw.num_values = col_chunk.num_values().max(0) as u64;
    raw.byte_range_start = byte_range_start;
    raw.total_compressed = total_compressed;

    let (ool_min, ool_max) = apply_thrift_stats(&mut raw, m.statistics.as_ref());

    Ok(BuiltChunk { raw, ool_min, ool_max, bloom_filter_parquet })
}

/// Reads min/max/null/distinct counts straight from parquet's thrift
/// statistics and writes them into `raw`, returning any out-of-line bytes.
///
/// Inline vs OOL is gated purely by stat byte width (1..=8 bytes inline,
/// longer goes OOL): the QuestDB column type doesn't constrain placement,
/// because the read path (`can_skip_row_group`, `find_row_group_by_timestamp`)
/// already interprets the slot at parquet physical width and applies any
/// parquet-aware overlay (e.g., `is_date * MILLIS_PER_DAY`) on its own.
/// Stats bytes are passed through verbatim — no typed deserialization, no
/// re-serialization at convert time.
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

fn build_column_chunk_from_thrift(
    meta: &parquet2::thrift_format::ColumnMetaData,
) -> ParquetResult<BuiltChunk> {
    // byte_range_start: prefer dictionary_page_offset if present.
    let byte_range_start = meta
        .dictionary_page_offset
        .unwrap_or(meta.data_page_offset)
        .max(0) as u64;
    let total_compressed = meta.total_compressed_size.max(0) as u64;

    // Codec: thrift CompressionCodec → parquet2 Compression → our Codec enum.
    let codec = parquet2::compression::Compression::try_from(meta.codec)
        .map(Codec::from)
        .map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "unsupported compression codec: {:?}",
                meta.codec
            )
        })?;

    // Encodings: convert thrift Encoding values to parquet2 Encoding, then to EncodingMask.
    let p2_encodings: Vec<parquet2::encoding::Encoding> = meta
        .encodings
        .iter()
        .filter_map(|e| parquet2::encoding::Encoding::try_from(*e).ok())
        .collect();
    let encodings = EncodingMask::from(p2_encodings.as_slice());

    // Bloom filter: capture parquet-file location so the caller can read the
    // bitset and store it in the _pm out-of-line region.
    let bloom_filter_parquet = match (meta.bloom_filter_offset, meta.bloom_filter_length) {
        (Some(off), Some(len)) if off > 0 && len > 0 => Some((off.max(0) as u64, len as u32)),
        _ => None,
    };

    let mut raw = ColumnChunkRaw::zeroed();
    raw.codec = codec as u8;
    raw.encodings = encodings.0;
    raw.num_values = meta.num_values.max(0) as u64;
    raw.byte_range_start = byte_range_start;
    raw.total_compressed = total_compressed;

    let (ool_min, ool_max) = apply_thrift_stats(&mut raw, meta.statistics.as_ref());

    Ok(BuiltChunk { raw, ool_min, ool_max, bloom_filter_parquet })
}

/// Column metadata needed to build a `_pm` header.
///
/// Callers construct this from their own types (`Partition`, `QdbMeta`, etc.)
/// so that `parquet_metadata` doesn't depend on `parquet_write`.
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

/// Result of an incremental `_pm` update.
///
/// `bytes` is always append-only: the caller seeks to the existing file size
/// and writes them after the previous trailer. The previous bytes (including
/// the previous trailer) are left untouched, preserving the stale-reader
/// invariant that any earlier committed `parquet_meta_file_size` continues to
/// resolve to a consistent older snapshot.
///
/// After appending `bytes`, the caller must patch `new_file_size` into the
/// header at `HEADER_PARQUET_META_FILE_SIZE_OFF` as the last step — this is
/// the MVCC commit signal that publishes the new snapshot.
#[derive(Debug)]
pub struct ParquetMetaUpdateResult {
    /// Bytes to append at the existing file size.
    pub bytes: Vec<u8>,
    /// Total `_pm` file size after the append. Also the value the caller must
    /// patch into the header at `HEADER_PARQUET_META_FILE_SIZE_OFF`.
    pub new_file_size: u64,
}

/// Generates a complete `_pm` file from pre-built column descriptors and raw
/// thrift row groups, inlining the bloom bitsets captured during the parquet
/// write.
#[allow(clippy::too_many_arguments)]
pub fn generate_parquet_metadata(
    columns: &[ParquetMetaColumnInfo<'_>],
    thrift_row_groups: &[RowGroup],
    designated_timestamp: i32,
    sorting_columns: &[u32],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_bitsets: &[Vec<Option<Vec<u8>>>],
    unused_bytes: u64,
    squash_tracker: i64,
    seq_txn: SeqTxn,
) -> ParquetResult<(Vec<u8>, u64)> {
    let bloom_source = VecBloomFilterSource::new(bloom_bitsets);
    qdb_parquet_meta::convert::generate_parquet_metadata(
        columns,
        thrift_row_groups,
        designated_timestamp,
        sorting_columns,
        parquet_footer_offset,
        parquet_footer_length,
        unused_bytes,
        squash_tracker,
        seq_txn,
        &bloom_source,
    )
    .map_err(ParquetError::from)
}

/// Updates an existing `_pm` file incrementally (append-only).
///
/// Compares the new parquet row groups against the existing `_pm` to
/// determine which row groups are unchanged, changed, or new. Unchanged row
/// groups keep their original offsets (no data rewritten). Only new/changed
/// blocks and a new footer are appended after the existing trailer, leaving
/// the previous bytes intact so that any older committed
/// `parquetMetaFileSize` continues to resolve to a consistent older snapshot.
///
/// Returns an error when the new row group count is smaller than the existing
/// one: the writer's row-group entry list cannot drop existing references, so
/// the caller must escalate to a full rewrite.
#[allow(clippy::too_many_arguments)]
pub fn update_parquet_metadata(
    existing_parquet_meta: &[u8],
    existing_parquet_meta_file_size: u64,
    thrift_row_groups: &[RowGroup],
    parquet_footer_offset: u64,
    parquet_footer_length: u32,
    bloom_bitsets: &[Vec<Option<Vec<u8>>>],
    unused_bytes: u64,
    seq_txn: SeqTxn,
) -> ParquetResult<ParquetMetaUpdateResult> {
    let existing_parquet_meta_len =
        usize::try_from(existing_parquet_meta_file_size).map_err(|_| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "_pm file size {} exceeds addressable range",
                existing_parquet_meta_file_size
            )
        })?;
    let existing_parquet_meta = existing_parquet_meta
        .get(..existing_parquet_meta_len)
        .ok_or_else(|| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Truncated,
                "_pm file size {} exceeds available data {}",
                existing_parquet_meta_file_size,
                existing_parquet_meta.len()
            )
        })?;

    let existing_reader = qdb_parquet_meta::reader::ParquetMetaReader::from_file_size(
        existing_parquet_meta,
        existing_parquet_meta_file_size,
    )?;
    let existing_rg_count = existing_reader.row_group_count() as usize;

    let mut existing_fingerprints: Vec<Option<u64>> = Vec::with_capacity(existing_rg_count);
    for i in 0..existing_rg_count {
        let rg = existing_reader.row_group(i)?;
        let fp = if existing_reader.column_count() > 0 {
            rg.column_chunk(0).map(|c| c.byte_range_start).ok()
        } else {
            None
        };
        existing_fingerprints.push(fp);
    }

    // Compaction is conceptually representable as an append (write a new
    // footer that references fewer row groups, leaving the dropped blocks as
    // dead space), but the current `ParquetMetaUpdateWriter` exposes only
    // replace/add — it has no remove operation, and the loop below only
    // iterates `thrift_row_groups`, so existing entries beyond the new length
    // would leak into the new footer as stale references. None of the Java
    // callers in `O3PartitionJob` shrink the row group count today, so this
    // is a defensive guard rather than a load-bearing check; it stays so any
    // future caller that violates the invariant fails loudly instead of
    // silently corrupting the file.
    if thrift_row_groups.len() < existing_rg_count {
        return Err(parquet_meta_err!(
            ParquetMetaErrorKind::InvalidValue,
            "_pm in-place update cannot shrink row group count ({} -> {}); caller must escalate to rewrite mode (new nameTxn directory)",
            existing_rg_count,
            thrift_row_groups.len()
        ));
    }

    let mut updater = qdb_parquet_meta::writer::ParquetMetaUpdateWriter::new(
        existing_parquet_meta,
        existing_parquet_meta_file_size,
    )?;

    let bloom_source = VecBloomFilterSource::new(bloom_bitsets);
    for (i, thrift_rg) in thrift_row_groups.iter().enumerate() {
        let new_fp: Option<u64> = thrift_rg
            .columns
            .first()
            .and_then(|c| c.meta_data.as_ref())
            .map(|m| m.dictionary_page_offset.unwrap_or(m.data_page_offset) as u64);

        if i < existing_rg_count && existing_fingerprints[i] == new_fp {
            continue;
        }

        let block = build_row_group_block(thrift_rg, i, &bloom_source)?;

        if i < existing_rg_count {
            updater.replace_row_group(i, block)?;
        } else {
            updater.add_row_group(block);
        }
    }

    updater.parquet_footer(parquet_footer_offset, parquet_footer_length);
    updater.unused_bytes(unused_bytes);
    updater.seq_txn(seq_txn);
    let (append_bytes, new_file_size) = updater.finish()?;
    debug_assert_eq!(
        new_file_size,
        existing_parquet_meta_file_size + append_bytes.len() as u64
    );

    Ok(ParquetMetaUpdateResult { bytes: append_bytes, new_file_size })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaColFormat};
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, ParquetEncodingConfig, Partition};
    use parquet2::compression::CompressionOptions;
    use parquet2::metadata::FileMetaData;
    use parquet2::read::read_metadata_with_size;
    use parquet2::schema::types::PhysicalType;
    use parquet2::write::Version;
    use qdb_core::col_type::ColumnTypeTag;
    use qdb_parquet_meta::reader::ParquetMetaReader;
    use qdb_parquet_meta::types::{Codec, ColumnFlags, FieldRepetition, StatFlags};
    use std::io::Cursor;

    fn write_test_parquet(row_count: usize, compression: CompressionOptions) -> Vec<u8> {
        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        };
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            id: 0,
            row_count,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: true,
            not_null_hint: true,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let mut buf = Vec::new();
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(compression)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count));

        writer.finish(partition).unwrap();
        buf
    }

    fn extract_qdb_meta_from(metadata: &FileMetaData) -> Option<QdbMeta> {
        metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == "questdb")
                    .and_then(|kv| kv.value.as_deref())
            })
            .map(|j| QdbMeta::deserialize(j).unwrap())
    }

    #[test]
    fn convert_simple_parquet() {
        let parquet_data = write_test_parquet(100, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 1);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.column_name(0).unwrap(), "ts");

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 100);

        let chunk = rg.column_chunk(0).unwrap();
        assert_eq!(chunk.codec().unwrap(), Codec::Uncompressed);
        assert!(chunk.byte_range_start > 0);
        assert!(chunk.total_compressed > 0);
        assert_eq!(chunk.num_values, 100);
    }

    #[test]
    fn convert_with_compression() {
        let parquet_data = write_test_parquet(1000, CompressionOptions::Snappy);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(0).unwrap();
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn convert_without_qdb_meta() {
        let parquet_data = write_test_parquet(50, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 1);
        let desc = reader.column_descriptor(0).unwrap();
        // Without QdbMeta, the type is inferred from the parquet schema.
        // The test parquet has a Timestamp(Micros) column → ColumnTypeTag::Timestamp (8).
        assert_eq!(desc.col_type, ColumnTypeTag::Timestamp as i32);
    }

    #[test]
    fn convert_propagates_squash_tracker_from_qdb_meta() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.squash_tracker = 42;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), Some(42));
    }

    #[test]
    fn convert_omits_squash_tracker_when_neg_one() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.squash_tracker = -1;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), None);
    }

    #[test]
    fn convert_without_qdb_meta_omits_squash_tracker() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, None, 0, 0, None).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.feature_flags().has_squash_tracker());
        assert_eq!(reader.squash_tracker(), None);
    }

    #[test]
    fn convert_propagates_seq_txn_from_qdb_meta() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.seq_txn = 77;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(reader.footer_feature_flags().has_seq_txn());
        assert_eq!(reader.seq_txn(), Some(SeqTxn::new(77)));
    }

    #[test]
    fn convert_omits_seq_txn_when_neg_one() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut qdb_meta = extract_qdb_meta_from(&metadata).expect("test parquet has qdb meta");
        qdb_meta.seq_txn = -1;

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)
                .unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(!reader.footer_feature_flags().has_seq_txn());
        assert_eq!(reader.seq_txn(), None);
    }

    #[test]
    fn convert_rejects_mismatched_schema() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut bad_meta = QdbMeta::new(0);
        bad_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Int.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
            });
        bad_meta
            .schema
            .push(crate::parquet::qdb_metadata::QdbMetaCol {
                column_type: ColumnTypeTag::Long.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
            });

        let result = convert_from_parquet(&metadata, Some(&bad_meta), 0, 0, None);
        assert!(result.is_err());
    }

    fn leak_bytes(data: &[u8]) -> &'static [u8] {
        Box::leak(data.to_vec().into_boxed_slice())
    }

    fn write_multi_column_parquet(row_count: usize) -> Vec<u8> {
        // Timestamp column (i64).
        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8)
        });

        // Int column (i32).
        let int_data: Vec<i32> = (0..row_count as i32).collect();
        let int_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(int_data.as_ptr() as *const u8, int_data.len() * 4)
        });

        // Double column (f64).
        let dbl_data: Vec<f64> = (0..row_count).map(|i| i as f64 * 1.5).collect();
        let dbl_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(dbl_data.as_ptr() as *const u8, dbl_data.len() * 8)
        });

        // Boolean column (u8, 1 byte per value).
        let bool_data: Vec<u8> = (0..row_count).map(|i| (i % 2) as u8).collect();
        let bool_bytes = leak_bytes(&bool_data);

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "int_val",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 1,
                row_count,
                primary_data: int_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "dbl_val",
                data_type: ColumnTypeTag::Double.into_type(),
                id: 2,
                row_count,
                primary_data: dbl_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "bool_val",
                data_type: ColumnTypeTag::Boolean.into_type(),
                id: 3,
                row_count,
                primary_data: bool_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];

        let partition = Partition { table: "test_multi".to_string(), columns: cols };

        let mut buf = Vec::new();
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count));

        writer.finish(partition).unwrap();
        buf
    }

    #[test]
    fn convert_multi_column_with_stats() {
        let parquet_data = write_multi_column_parquet(100);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 1024, 200, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 4);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.parquet_footer_offset(), 1024);
        assert_eq!(reader.parquet_footer_length(), 200);

        assert_eq!(reader.column_name(0).unwrap(), "ts");
        assert_eq!(reader.column_name(1).unwrap(), "int_val");
        assert_eq!(reader.column_name(2).unwrap(), "dbl_val");
        assert_eq!(reader.column_name(3).unwrap(), "bool_val");

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 100);

        let ts_chunk = rg.column_chunk(0).unwrap();
        let ts_flags = ts_chunk.stat_flags();
        assert!(ts_flags.has_min_stat());
        assert!(ts_flags.has_max_stat());
        assert!(ts_flags.has_null_count());
        assert_eq!(ts_chunk.num_values, 100);

        let int_chunk = rg.column_chunk(1).unwrap();
        let int_flags = int_chunk.stat_flags();
        assert!(int_flags.has_min_stat());
        assert!(int_flags.has_max_stat());

        let dbl_chunk = rg.column_chunk(2).unwrap();
        let dbl_flags = dbl_chunk.stat_flags();
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.has_max_stat());

        let bool_chunk = rg.column_chunk(3).unwrap();
        let bool_flags = bool_chunk.stat_flags();
        assert!(bool_flags.has_min_stat());
        assert!(bool_flags.has_max_stat());
    }

    /// Regression: a SHORT column with negative values must round-trip
    /// through the inline u64 stat slot and read back as the correct i32 at
    /// parquet physical width (Int32). Earlier the convert path narrowed to 2
    /// bytes, the inline slot zero-padded to 8, and the skip path's 4-byte
    /// i32 read turned a -74 i16 into 65462, dropping every row group whose
    /// true min was negative.
    #[test]
    fn convert_short_negative_min_round_trips_at_int32_width() {
        let row_count = 100;
        let short_data: Vec<i16> = (0..row_count as i16).map(|i| i - 50).collect();
        let short_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(short_data.as_ptr() as *const u8, short_data.len() * 2)
        });

        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8)
        });

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "val",
                data_type: ColumnTypeTag::Short.into_type(),
                id: 1,
                row_count,
                primary_data: short_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];

        let partition = Partition { table: "test_short_neg".to_string(), columns: cols };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(1).unwrap();

        let bytes = chunk.min_stat.to_le_bytes();
        let min_i32 = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(
            min_i32, -50,
            "inline min_stat must read back as -50 at Int32 width"
        );

        let bytes = chunk.max_stat.to_le_bytes();
        let max_i32 = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(
            max_i32, 49,
            "inline max_stat must read back as 49 at Int32 width"
        );
    }

    /// Regression: an external INT32 + Date logical-type parquet must keep
    /// its stats as i32 days at parquet physical width when ingested through
    /// the inline path.
    #[test]
    fn convert_int32_date_round_trips_at_int32_width() {
        use parquet2::thrift_format::{
            ColumnChunk, ColumnMetaData, CompressionCodec, Encoding as ThriftEncoding,
            RowGroup as ThriftRowGroup, Statistics as ThriftStatistics, Type,
        };

        let min_days: i32 = -100;
        let max_days: i32 = 365;

        let stats = ThriftStatistics {
            max: None,
            min: None,
            null_count: Some(0),
            distinct_count: None,
            max_value: Some(max_days.to_le_bytes().to_vec()),
            min_value: Some(min_days.to_le_bytes().to_vec()),
        };
        let meta = ColumnMetaData {
            type_: Type::INT32,
            encodings: vec![ThriftEncoding::PLAIN],
            path_in_schema: vec!["d".to_string()],
            codec: CompressionCodec::UNCOMPRESSED,
            num_values: 100,
            total_uncompressed_size: 400,
            total_compressed_size: 400,
            key_value_metadata: None,
            data_page_offset: 4,
            index_page_offset: None,
            dictionary_page_offset: None,
            statistics: Some(stats),
            encoding_stats: None,
            bloom_filter_offset: None,
            bloom_filter_length: None,
        };
        let column_chunk = ColumnChunk {
            file_path: None,
            file_offset: 0,
            meta_data: Some(meta),
            offset_index_offset: None,
            offset_index_length: None,
            column_index_offset: None,
            column_index_length: None,
            crypto_metadata: None,
            encrypted_column_metadata: None,
        };
        let row_group = ThriftRowGroup {
            columns: vec![column_chunk],
            total_byte_size: 400,
            num_rows: 100,
            sorting_columns: None,
            file_offset: None,
            total_compressed_size: None,
            ordinal: None,
        };

        let block = build_row_group_block(&row_group, 0, &NoBloomFilterSource).unwrap();

        let chunk = block.column_chunk_raw(0);
        let stat_flags = StatFlags(chunk.stat_flags);
        assert!(stat_flags.has_min_stat());
        assert!(stat_flags.is_min_inlined());
        assert!(stat_flags.has_max_stat());
        assert!(stat_flags.is_max_inlined());

        let min_bytes = chunk.min_stat.to_le_bytes();
        let min_i32 = i32::from_le_bytes([min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3]]);
        assert_eq!(min_i32, min_days);
        let max_bytes = chunk.max_stat.to_le_bytes();
        let max_i32 = i32::from_le_bytes([max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3]]);
        assert_eq!(max_i32, max_days);

        assert_eq!(&min_bytes[4..], &[0u8; 4]);
        assert_eq!(&max_bytes[4..], &[0u8; 4]);
    }

    #[test]
    fn convert_with_qdb_meta_flags() {
        let parquet_data = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let mut meta = QdbMeta::new(1);
        meta.schema.push(crate::parquet::qdb_metadata::QdbMetaCol {
            column_type: ColumnTypeTag::Symbol.into_type(),
            column_top: 42,
            format: Some(QdbMetaColFormat::LocalKeyIsGlobal),
            ascii: Some(true),
        });

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&meta), 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let desc = reader.column_descriptor(0).unwrap();
        let flags = desc.flags();
        assert!(flags.is_local_key_global());
        assert!(flags.is_ascii());
    }

    #[test]
    fn convert_sorting_columns_propagated() {
        let parquet_data = write_test_parquet(100, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();

        let has_sorting = metadata.row_groups.iter().any(|rg| {
            rg.sorting_columns()
                .as_ref()
                .is_some_and(|sc| !sc.is_empty())
        });

        let qdb_meta = extract_qdb_meta_from(&metadata);
        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();

        if has_sorting {
            assert!(reader.sorting_column_count() > 0);
            assert_eq!(reader.sorting_column(0).unwrap(), 0);
        } else {
            assert_eq!(reader.sorting_column_count(), 0);
        }

        assert!(reader.designated_timestamp().is_some());
    }

    #[test]
    fn convert_multi_row_groups() {
        let row_count = 200;
        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        });

        let col = Column {
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            id: 0,
            row_count,
            primary_data: data_bytes,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: true,
            not_null_hint: true,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let mut buf = Vec::new();
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(75));

        writer.finish(partition).unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();

        assert!(metadata.row_groups.len() >= 2);

        let qdb_meta = extract_qdb_meta_from(&metadata);
        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.row_group_count(), metadata.row_groups.len() as u32);

        for i in 0..reader.row_group_count() as usize {
            let rg = reader.row_group(i).unwrap();
            assert_eq!(rg.num_rows(), metadata.row_groups[i].num_rows() as u64);
        }
    }

    fn write_float_parquet(row_count: usize) -> Vec<u8> {
        let float_data: Vec<f32> = (0..row_count).map(|i| i as f32 * 0.5).collect();
        let float_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(float_data.as_ptr() as *const u8, float_data.len() * 4)
        });

        let col = Column {
            name: "float_val",
            data_type: ColumnTypeTag::Float.into_type(),
            id: 0,
            row_count,
            primary_data: float_bytes,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: false,
            not_null_hint: false,
            designated_timestamp_ascending: false,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition {
            table: "test_float".to_string(),
            columns: vec![col],
        };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .finish(partition)
            .unwrap();
        buf
    }

    #[test]
    fn convert_float_column_stats() {
        let parquet_data = write_float_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let rg = reader.row_group(0).unwrap();
        let chunk = rg.column_chunk(0).unwrap();
        let flags = chunk.stat_flags();
        assert!(flags.has_min_stat());
        assert!(flags.has_max_stat());
        assert!(flags.is_min_inlined());
        assert!(flags.is_max_inlined());
    }

    #[test]
    fn convert_distinct_count_present() {
        let parquet_data = write_multi_column_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let rg = reader.row_group(0).unwrap();

        let mut has_null_count = false;
        for i in 0..reader.column_count() as usize {
            let chunk = rg.column_chunk(i).unwrap();
            if chunk.stat_flags().has_null_count() {
                has_null_count = true;
            }
        }
        assert!(has_null_count);
    }

    /// Verifies that the FileMetaData path (`convert_from_parquet`) and the
    /// raw thrift path (`build_row_group_block`) produce identical row-group
    /// blocks for the same parquet input.
    #[test]
    fn thrift_round_trip_matches_convert_from_parquet() {
        let parquet_data = write_multi_column_parquet(200);
        let mut cursor = Cursor::new(&parquet_data);
        let file_size = parquet_data.len() as u64;
        let metadata = read_metadata_with_size(&mut cursor, file_size).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        // Path 1: convert_from_parquet
        let (parquet_meta_bytes_from_meta, parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None).unwrap();
        let reader1 = ParquetMetaReader::from_file_size(
            &parquet_meta_bytes_from_meta,
            parquet_meta_file_size,
        )
        .unwrap();

        let parquet_data2 = write_multi_column_parquet(200);
        let mut cursor2 = Cursor::new(&parquet_data2);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data2.len() as u64).unwrap();
        let thrift_meta = metadata2.into_thrift();

        for (rg_idx, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block = build_row_group_block(thrift_rg, rg_idx, &NoBloomFilterSource).unwrap();

            let rg1 = reader1.row_group(rg_idx).unwrap();
            assert_eq!(block.num_rows(), rg1.num_rows());

            for col_idx in 0..reader1.column_count() as usize {
                let chunk1 = rg1.column_chunk(col_idx).unwrap();
                let chunk2 = block.column_chunk_raw(col_idx);

                assert_eq!(chunk1.codec, chunk2.codec);
                assert_eq!(chunk1.encodings, chunk2.encodings);
                assert_eq!(chunk1.byte_range_start, chunk2.byte_range_start);
                assert_eq!(chunk1.total_compressed, chunk2.total_compressed);
                assert_eq!(chunk1.num_values, chunk2.num_values);
                assert_eq!(chunk1.null_count, chunk2.null_count);
                assert_eq!(chunk1.stat_flags, chunk2.stat_flags);
                assert_eq!(chunk1.min_stat, chunk2.min_stat);
                assert_eq!(chunk1.max_stat, chunk2.max_stat);
            }
        }
    }

    #[test]
    fn thrift_multi_column_with_stats() {
        let parquet_data = write_multi_column_parquet(50);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block =
            build_row_group_block(&thrift_meta.row_groups[0], 0, &NoBloomFilterSource).unwrap();

        assert_eq!(block.num_rows(), 50);

        let ts_chunk = block.column_chunk_raw(0);
        let ts_flags = StatFlags(ts_chunk.stat_flags);
        assert!(ts_flags.has_min_stat());
        assert!(ts_flags.is_min_inlined());
        assert!(ts_flags.has_max_stat());
        assert!(ts_flags.is_max_inlined());
        assert!(ts_flags.has_null_count());
        assert_eq!(ts_chunk.null_count, 0);
        assert_eq!(ts_chunk.min_stat, 0);
        assert_eq!(ts_chunk.max_stat, 49);

        let int_chunk = block.column_chunk_raw(1);
        let int_flags = StatFlags(int_chunk.stat_flags);
        assert!(int_flags.has_min_stat());
        assert!(int_flags.is_min_inlined());
        assert_eq!(int_chunk.min_stat as i32, 0);
        assert_eq!(int_chunk.max_stat as i32, 49);

        let dbl_chunk = block.column_chunk_raw(2);
        let dbl_flags = StatFlags(dbl_chunk.stat_flags);
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.is_min_inlined());
        assert_eq!(f64::from_le_bytes(dbl_chunk.min_stat.to_le_bytes()), 0.0);
        assert_eq!(f64::from_le_bytes(dbl_chunk.max_stat.to_le_bytes()), 73.5);

        let flag_chunk = block.column_chunk_raw(3);
        let flag_flags = StatFlags(flag_chunk.stat_flags);
        assert!(flag_flags.has_min_stat());
        assert!(flag_flags.is_min_inlined());
    }

    #[test]
    fn thrift_with_compression() {
        let parquet_data = write_test_parquet(100, CompressionOptions::Snappy);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block =
            build_row_group_block(&thrift_meta.row_groups[0], 0, &NoBloomFilterSource).unwrap();

        let chunk = block.column_chunk_raw(0);
        assert_eq!(chunk.codec().unwrap(), Codec::Snappy);
    }

    #[test]
    fn thrift_without_qdb_meta_infers_types() {
        let parquet_data = write_multi_column_parquet(30);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        let block =
            build_row_group_block(&thrift_meta.row_groups[0], 0, &NoBloomFilterSource).unwrap();

        let ts_chunk = block.column_chunk_raw(0);
        let ts_flags = StatFlags(ts_chunk.stat_flags);
        assert!(ts_flags.has_min_stat());
        assert!(ts_flags.is_min_inlined());

        let int_chunk = block.column_chunk_raw(1);
        let int_flags = StatFlags(int_chunk.stat_flags);
        assert!(int_flags.has_min_stat());
        assert!(int_flags.is_min_inlined());

        let dbl_chunk = block.column_chunk_raw(2);
        let dbl_flags = StatFlags(dbl_chunk.stat_flags);
        assert!(dbl_flags.has_min_stat());
        assert!(dbl_flags.is_min_inlined());

        let flag_chunk = block.column_chunk_raw(3);
        let flag_flags = StatFlags(flag_chunk.stat_flags);
        assert!(flag_flags.has_min_stat());
        assert!(flag_flags.is_min_inlined());
    }

    #[test]
    fn thrift_multiple_row_groups() {
        let ts_data: Vec<i64> = (0..100i64).collect();
        let ts_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8) }
                .to_vec()
                .into_boxed_slice(),
        );

        let partition = Partition {
            table: "test".to_string(),
            columns: vec![Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count: 100,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            }],
        };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_row_group_size(Some(30))
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let thrift_meta = metadata.into_thrift();

        assert_eq!(thrift_meta.row_groups.len(), 4);
        let expected_rows = [30u64, 30, 30, 10];

        for (i, thrift_rg) in thrift_meta.row_groups.iter().enumerate() {
            let block = build_row_group_block(thrift_rg, i, &NoBloomFilterSource).unwrap();
            assert_eq!(block.num_rows(), expected_rows[i]);
        }
    }

    fn col_infos_from_schema<'a>(
        schema_columns: &'a [parquet2::metadata::ColumnDescriptor],
        qdb_meta: Option<&'a QdbMeta>,
    ) -> Vec<ParquetMetaColumnInfo<'a>> {
        schema_columns
            .iter()
            .enumerate()
            .map(|(i, col_desc)| {
                let field_info = col_desc.base_type.get_field_info();
                let cm = qdb_meta.and_then(|m| m.schema.get(i));
                let mut flags = ColumnFlags::new();
                flags = flags.with_repetition(FieldRepetition::from(field_info.repetition));
                ParquetMetaColumnInfo {
                    name: &field_info.name,
                    col_type_code: cm.map(|c| c.column_type.code()).unwrap_or_else(|| {
                        crate::parquet_read::meta::infer_column_type(col_desc)
                            .map(|ct| ct.code())
                            .unwrap_or(-1)
                    }),
                    id: field_info.id.unwrap_or(-1),
                    flags,
                    fixed_byte_len: match col_desc.descriptor.primitive_type.physical_type {
                        PhysicalType::FixedLenByteArray(len) => len as i32,
                        _ => 0,
                    },
                    physical_type: physical_type_to_u8(
                        col_desc.descriptor.primitive_type.physical_type,
                    ),
                    max_rep_level: col_desc.descriptor.max_rep_level as u8,
                    max_def_level: col_desc.descriptor.max_def_level as u8,
                }
            })
            .collect()
    }

    #[test]
    fn generate_parquet_metadata_produces_valid_pm() {
        let parquet_data = write_multi_column_parquet(80);
        let file_size = parquet_data.len() as u64;
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, file_size).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

        let parquet_footer_offset = 100u64;
        let parquet_footer_length = 50u32;

        let (parquet_meta_bytes, parquet_meta_file_size) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            parquet_footer_offset,
            parquet_footer_length,
            &[],
            0,
            -1,
            SeqTxn::UNSET,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.column_count(), 4);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.designated_timestamp(), Some(0));
        assert_eq!(reader.sorting_column_count(), 1);
        assert_eq!(reader.parquet_footer_offset(), parquet_footer_offset);
        assert_eq!(reader.parquet_footer_length(), parquet_footer_length);
        assert_eq!(reader.column_name(0).unwrap(), "ts");
        assert_eq!(reader.column_name(1).unwrap(), "int_val");

        let rg = reader.row_group(0).unwrap();
        assert_eq!(rg.num_rows(), 80);
        assert!(reader.verify_checksum().is_ok());
        assert_eq!(parquet_meta_file_size, parquet_meta_bytes.len() as u64);
    }

    #[test]
    fn update_parquet_metadata_appends_new_row_group() {
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

        let (initial_pm, _) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
            SeqTxn::UNSET,
        )
        .unwrap();

        let initial_size = initial_pm.len() as u64;
        let initial_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(initial_reader.row_group_count(), 1);

        let mut extended_rgs = thrift_meta.row_groups.clone();
        let mut new_rg = extended_rgs[0].clone();
        for col in &mut new_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 10_000;
            }
        }
        extended_rgs.push(new_rg);

        let result = update_parquet_metadata(
            &initial_pm,
            initial_size,
            &extended_rgs,
            200,
            60,
            &[],
            0,
            SeqTxn::UNSET,
        )
        .unwrap();

        assert!(!result.bytes.is_empty());

        let mut full_file = initial_pm.clone();
        full_file.extend_from_slice(&result.bytes);
        assert_eq!(full_file.len() as u64, result.new_file_size);
        full_file[qdb_parquet_meta::types::HEADER_PARQUET_META_FILE_SIZE_OFF
            ..qdb_parquet_meta::types::HEADER_PARQUET_META_FILE_SIZE_OFF + 8]
            .copy_from_slice(&result.new_file_size.to_le_bytes());

        let new_reader =
            ParquetMetaReader::from_file_size(&full_file, result.new_file_size).unwrap();
        assert_eq!(new_reader.row_group_count(), 2);
        assert!(new_reader.verify_checksum().is_ok());

        let old_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(old_reader.row_group_count(), 1);
    }

    #[test]
    fn update_with_decreasing_row_group_count_returns_error() {
        let parquet_data = write_multi_column_parquet(80);
        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&parquet_data);
        let metadata2 = read_metadata_with_size(&mut cursor2, parquet_data.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

        let mut three_rgs = thrift_meta.row_groups.clone();
        let mut second_rg = three_rgs[0].clone();
        for col in &mut second_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 10_000;
            }
        }
        let mut third_rg = three_rgs[0].clone();
        for col in &mut third_rg.columns {
            if let Some(ref mut meta) = col.meta_data {
                meta.data_page_offset += 20_000;
            }
        }
        three_rgs.push(second_rg);
        three_rgs.push(third_rg);
        assert_eq!(three_rgs.len(), 3);

        let (initial_pm, _) = generate_parquet_metadata(
            &col_infos,
            &three_rgs,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
            SeqTxn::UNSET,
        )
        .unwrap();
        let initial_size = initial_pm.len() as u64;

        let initial_reader = ParquetMetaReader::from_file_size(&initial_pm, initial_size).unwrap();
        assert_eq!(initial_reader.row_group_count(), 3);

        let two_rgs = three_rgs[..2].to_vec();
        let result = update_parquet_metadata(
            &initial_pm,
            initial_size,
            &two_rgs,
            100,
            50,
            &[],
            0,
            SeqTxn::UNSET,
        );

        let err = match result {
            Ok(_) => panic!("update should fail when row groups shrink"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("cannot shrink row group count (3 -> 2)"),
            "expected 'cannot shrink row group count (3 -> 2)' in error, got: {msg}"
        );
        assert!(
            msg.contains("escalate to rewrite mode"),
            "expected escalation hint in error, got: {msg}"
        );
    }

    #[test]
    fn thrift_missing_column_metadata_errors() {
        let rg = parquet2::thrift_format::RowGroup {
            columns: vec![parquet2::thrift_format::ColumnChunk {
                file_path: None,
                file_offset: 0,
                meta_data: None,
                offset_index_offset: None,
                offset_index_length: None,
                column_index_offset: None,
                column_index_length: None,
                crypto_metadata: None,
                encrypted_column_metadata: None,
            }],
            total_byte_size: 0,
            num_rows: 10,
            sorting_columns: None,
            file_offset: None,
            total_compressed_size: None,
            ordinal: None,
        };

        let result = build_row_group_block(&rg, 0, &NoBloomFilterSource);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("no metadata"), "got: {err_msg}");
    }

    #[test]
    fn bloom_filter_extracted_from_parquet_into_pm() {
        let row_count = 100;
        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        };
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            id: 0,
            row_count,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: true,
            not_null_hint: true,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };

        let partition = Partition {
            table: "test_bloom".to_string(),
            columns: vec![col],
        };
        let mut buf = Vec::new();
        let writer = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_bloom_filter_columns([0].into_iter().collect())
            .with_bloom_filter_fpp(0.01)
            .with_row_group_size(Some(row_count));

        let (schema, additional_meta) =
            crate::parquet_write::schema::to_parquet_schema(&partition, false, -1, -1).unwrap();
        let encodings = crate::parquet_write::schema::to_encodings(&partition);
        let compressions = crate::parquet_write::schema::to_compressions(&partition);
        let mut chunked = writer
            .chunked_with_compressions(schema, encodings, compressions)
            .unwrap();
        chunked.write_chunk(&partition).unwrap();
        chunked.finish(additional_meta).unwrap();

        let bloom_bitsets = chunked.bloom_bitsets();
        assert!(
            bloom_bitsets.len() == 1 && bloom_bitsets[0][0].is_some(),
            "should have captured bloom filter bitset"
        );

        let col_infos = vec![ParquetMetaColumnInfo {
            name: "ts",
            col_type_code: ColumnTypeTag::Timestamp.into_type().code(),
            id: 0,
            flags: ColumnFlags::new().with_repetition(FieldRepetition::Required),
            fixed_byte_len: 0,
            physical_type: physical_type_to_u8(PhysicalType::Int64),
            max_rep_level: 0,
            max_def_level: 0,
        }];

        let (parquet_meta_bytes, parquet_meta_file_size) = generate_parquet_metadata(
            &col_infos,
            chunked.row_groups(),
            0,
            &[0],
            100,
            50,
            bloom_bitsets,
            0,
            -1,
            SeqTxn::UNSET,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(reader.row_group_count(), 1);
        assert!(reader.has_bloom_filters());
        assert_eq!(reader.bloom_filter_position(0), Some(0));

        let bf_abs = reader.bloom_filter_offset_in_pm(0, 0).unwrap() as usize;
        assert_ne!(bf_abs, 0);
        assert_eq!(bf_abs % 8, 0);
        assert!(bf_abs + 4 <= parquet_meta_bytes.len());

        let bf_data = &parquet_meta_bytes[bf_abs..];
        let bf_len = i32::from_le_bytes(bf_data[..4].try_into().unwrap()) as usize;
        assert!(bf_len >= 32);
        assert!(bf_abs + 4 + bf_len <= parquet_meta_bytes.len());

        let inlined = &bf_data[4..4 + bf_len];
        let captured = bloom_bitsets[0][0].as_ref().unwrap();
        assert_eq!(
            inlined,
            captured.as_slice(),
            "inlined `_pm` bloom bitset must equal the bitset captured during parquet write"
        );
    }

    /// Writes a parquet file with a bloom filter on the `id` column. Returns
    /// the raw parquet bytes; the migration path mmaps these bytes and hands
    /// them to `convert_from_parquet` as `parquet_file_data`.
    fn write_parquet_with_bloom_filter() -> Vec<u8> {
        let row_count = 100usize;
        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8) }
                .to_vec()
                .into_boxed_slice(),
        );
        let id_data: Vec<i32> = (0..row_count as i32).collect();
        let id_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(id_data.as_ptr() as *const u8, id_data.len() * 4) }
                .to_vec()
                .into_boxed_slice(),
        );

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "id",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 1,
                row_count,
                primary_data: id_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: true,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];
        let partition = Partition { table: "bloom".to_string(), columns: cols };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .with_bloom_filter_columns([1].into_iter().collect())
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .unwrap();
        buf
    }

    /// Migration path: when the caller passes the parquet bytes, the bloom
    /// filter bitset is read out of the parquet footer and inlined into the
    /// `_pm` out-of-line region. Covers the new `parquet_file_data: Some(...)`
    /// branch in `convert_from_parquet`.
    #[test]
    fn convert_from_parquet_inlines_bloom_filter_from_slice() {
        let parquet_data = write_parquet_with_bloom_filter();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        // Sanity: the parquet footer carries a bloom_filter_offset for `id`.
        let id_meta = metadata.row_groups[0].columns()[1].metadata();
        assert!(
            id_meta.bloom_filter_offset.is_some_and(|o| o > 0),
            "test parquet should carry a bloom_filter_offset on the id column"
        );

        let (pm_bytes, pm_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            None,
            Some(parquet_data.as_slice()),
        )
        .unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).unwrap();
        assert!(
            reader.has_bloom_filters(),
            "BLOOM_FILTERS feature flag should be set when bloom filters are inlined"
        );
        // Column 0 (ts) has no bloom filter; column 1 (id) does. The footer
        // section is one entry deep.
        assert_eq!(reader.bloom_filter_position(0), None);
        let id_pos = reader
            .bloom_filter_position(1)
            .expect("id column should have a bloom filter footer entry");
        let bf_abs = reader.bloom_filter_offset_in_pm(0, id_pos).unwrap() as usize;
        assert_ne!(bf_abs, 0, "inlined bloom filter offset should be non-zero");
        let bf_len = i32::from_le_bytes(pm_bytes[bf_abs..bf_abs + 4].try_into().unwrap()) as usize;
        assert!(bf_len >= 32, "inlined bitset must be at least 32 bytes");
        let inlined_bitset = &pm_bytes[bf_abs + 4..bf_abs + 4 + bf_len];
        assert!(
            inlined_bitset.iter().any(|&b| b != 0),
            "inlined bloom bitset should have at least one bit set for 100 values"
        );
    }

    /// Migration path: when the caller does NOT pass parquet bytes (i.e. it
    /// has not mmapped the file), bloom filters are not inlined and `_pm`
    /// does not carry the BLOOM_FILTERS feature flag, even though the parquet
    /// footer would have allowed it. Anchors the `parquet_file_data: None`
    /// branch so a future change that flips it on by default fails the test.
    #[test]
    fn convert_from_parquet_skips_bloom_inline_without_parquet_data() {
        let parquet_data = write_parquet_with_bloom_filter();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let (pm_bytes, pm_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, None).unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).unwrap();
        assert!(
            !reader.has_bloom_filters(),
            "BLOOM_FILTERS flag must stay off when parquet_file_data is None"
        );
    }

    /// Migration path error case: the caller passes a truncated view of the
    /// parquet file that does not extend to the bloom_filter_offset recorded
    /// in the footer. `parquet2::bloom_filter::read_from_slice_at_offset`
    /// rejects this, the converter wraps the error with a Conversion kind,
    /// and the failure surfaces to the caller rather than silently producing
    /// a `_pm` without the bloom filter the footer claims. Covers the
    /// `map_err` branch in the bloom-inline block.
    #[test]
    fn convert_from_parquet_propagates_bloom_read_error_on_truncated_slice() {
        let parquet_data = write_parquet_with_bloom_filter();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);

        let bloom_offset = metadata.row_groups[0].columns()[1]
            .metadata()
            .bloom_filter_offset
            .expect("test parquet should carry a bloom_filter_offset")
            as usize;
        // Truncate the parquet bytes before the bloom-filter region so the
        // converter's read of the bitset at `bloom_offset` fails.
        let truncated = &parquet_data[..bloom_offset.saturating_sub(1)];

        let err = convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, None, Some(truncated))
            .expect_err("truncated parquet_file_data should fail bloom-filter read");

        let msg = format!("{err}");
        assert!(
            msg.contains("could not read parquet bloom filter at offset"),
            "error message should mention bloom filter read failure, got: {msg}"
        );
    }

    /// Migration path: a parquet with two bloom-filtered columns split across
    /// two row groups must inline a distinct bitset for every (row group,
    /// column) pair. Guards against a regression that copies one bitset to all
    /// row groups, drops all but the last, or overlaps per-column bitsets in
    /// the `_pm` out-of-line region.
    #[test]
    fn convert_from_parquet_inlines_bloom_filters_across_row_groups_and_columns() {
        let parquet_data = write_parquet_with_two_bloom_columns_two_row_groups();

        let mut cursor = Cursor::new(&parquet_data);
        let metadata = read_metadata_with_size(&mut cursor, parquet_data.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        assert_eq!(
            metadata.row_groups.len(),
            2,
            "test parquet should have two row groups"
        );

        let (pm_bytes, pm_file_size) = convert_from_parquet(
            &metadata,
            qdb_meta.as_ref(),
            0,
            0,
            None,
            Some(&parquet_data),
        )
        .unwrap();

        let reader = ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).unwrap();
        assert!(reader.has_bloom_filters());
        assert_eq!(
            reader.bloom_filter_position(0),
            None,
            "ts column carries no bloom filter"
        );
        let pos_a = reader
            .bloom_filter_position(1)
            .expect("column 1 should have a bloom filter");
        let pos_b = reader
            .bloom_filter_position(2)
            .expect("column 2 should have a bloom filter");

        let mut offsets = Vec::with_capacity(4);
        for rg in 0..2usize {
            for pos in [pos_a, pos_b] {
                let off = reader.bloom_filter_offset_in_pm(rg, pos).unwrap() as usize;
                assert_ne!(off, 0, "each (row group, column) pair must inline a bitset");
                let len = i32::from_le_bytes(pm_bytes[off..off + 4].try_into().unwrap()) as usize;
                assert!(len >= 32, "inlined bitset must be at least 32 bytes");
                let bitset = &pm_bytes[off + 4..off + 4 + len];
                assert!(
                    bitset.iter().any(|&b| b != 0),
                    "inlined bloom bitset should have at least one bit set"
                );
                offsets.push(off);
            }
        }
        let mut distinct = offsets.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            offsets.len(),
            "every (row group, column) bloom bitset must occupy a distinct _pm offset"
        );
    }

    fn write_parquet_with_two_bloom_columns_two_row_groups() -> Vec<u8> {
        let row_count = 100usize;
        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8) }
                .to_vec()
                .into_boxed_slice(),
        );
        let a_data: Vec<i32> = (0..row_count as i32).collect();
        let a_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(a_data.as_ptr() as *const u8, a_data.len() * 4) }
                .to_vec()
                .into_boxed_slice(),
        );
        let b_data: Vec<i32> = (0..row_count as i32)
            .map(|x| x.wrapping_mul(7) + 3)
            .collect();
        let b_bytes: &'static [u8] = Box::leak(
            unsafe { std::slice::from_raw_parts(b_data.as_ptr() as *const u8, b_data.len() * 4) }
                .to_vec()
                .into_boxed_slice(),
        );

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "a",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 1,
                row_count,
                primary_data: a_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: true,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "b",
                data_type: ColumnTypeTag::Int.into_type(),
                id: 2,
                row_count,
                primary_data: b_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: true,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];
        let partition = Partition { table: "bloom".to_string(), columns: cols };

        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            // Half the rows per group yields two row groups.
            .with_row_group_size(Some(row_count / 2))
            .with_bloom_filter_columns([1, 2].into_iter().collect())
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .unwrap();
        buf
    }

    #[test]
    fn uuid_ool_stats_round_trip() {
        let row_count = 10usize;
        let uuid_data: Vec<[u8; 16]> = (0..row_count)
            .map(|i| {
                let mut buf = [0u8; 16];
                buf[0..8].copy_from_slice(&((i as u64 + 1) * 0x1111).to_le_bytes());
                buf[8..16].copy_from_slice(&0u64.to_le_bytes());
                buf
            })
            .collect();
        let uuid_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(uuid_data.as_ptr() as *const u8, row_count * 16)
        });

        let ts_data: Vec<i64> = (0..row_count as i64).collect();
        let ts_bytes = leak_bytes(unsafe {
            std::slice::from_raw_parts(ts_data.as_ptr() as *const u8, ts_data.len() * 8)
        });

        let cols = vec![
            Column {
                name: "ts",
                data_type: ColumnTypeTag::Timestamp.into_type(),
                id: 0,
                row_count,
                primary_data: ts_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: true,
                not_null_hint: true,
                designated_timestamp_ascending: true,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
            Column {
                name: "val",
                data_type: ColumnTypeTag::Uuid.into_type(),
                id: 1,
                row_count,
                primary_data: uuid_bytes,
                secondary_data: &[],
                symbol_offsets: &[],
                column_top: 0,
                designated_timestamp: false,
                not_null_hint: false,
                designated_timestamp_ascending: false,
                parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
            },
        ];
        let partition = Partition { table: "test".to_string(), columns: cols };
        let mut buf = Vec::new();
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_version(Version::V1)
            .with_row_group_size(Some(row_count))
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64).unwrap();
        let qdb_meta = extract_qdb_meta_from(&metadata);
        let thrift_meta = metadata.into_thrift();

        let mut cursor2 = Cursor::new(&buf);
        let metadata2 = read_metadata_with_size(&mut cursor2, buf.len() as u64).unwrap();
        let schema_columns = metadata2.schema_descr.columns();

        let col_infos = col_infos_from_schema(schema_columns, qdb_meta.as_ref());

        let (parquet_meta_bytes, _) = generate_parquet_metadata(
            &col_infos,
            &thrift_meta.row_groups,
            0,
            &[0],
            100,
            50,
            &[],
            0,
            -1,
            SeqTxn::UNSET,
        )
        .unwrap();

        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_bytes.len() as u64)
                .unwrap();
        assert_eq!(reader.column_count(), 2);
        assert_eq!(reader.column_name(1).unwrap(), "val");

        let rg = reader.row_group(0).unwrap();
        let chunk = rg.column_chunk(1).unwrap();
        let stat_flags = StatFlags(chunk.stat_flags);

        let ool = rg.out_of_line_region();

        assert!(stat_flags.has_min_stat());
        assert!(!stat_flags.is_min_inlined());
        assert!(stat_flags.has_max_stat());
        assert!(!stat_flags.is_max_inlined());
        assert!(!ool.is_empty());

        let min_off = (chunk.min_stat >> 16) as usize;
        let min_len = (chunk.min_stat & 0xFFFF) as usize;
        let max_off = (chunk.max_stat >> 16) as usize;
        let max_len = (chunk.max_stat & 0xFFFF) as usize;
        assert_eq!(min_len, 16);
        assert_eq!(max_len, 16);
        let min_bytes = &ool[min_off..min_off + min_len];
        let max_bytes = &ool[max_off..max_off + max_len];
        assert_ne!(min_bytes, max_bytes);
    }

    /// Writes a parquet file with a single i64 "ts" column holding no stats
    /// and a hand-built QdbMeta that marks col 0 as the designated timestamp.
    /// The column has `designated_timestamp: false` so the writer's hard-coded
    /// "designated ts always gets stats" override at `parquet_write/file.rs`
    /// does not fire; `with_statistics(false)` then suppresses all stats.
    fn write_parquet_without_ts_stats(
        row_count: usize,
        rows_per_group: usize,
    ) -> (Vec<u8>, QdbMeta) {
        use crate::parquet::qdb_metadata::{QdbMetaCol, QDB_META_KEY};
        use crate::parquet_write::schema::{to_compressions, to_encodings, to_parquet_schema};
        use parquet2::metadata::KeyValue;

        let col_data: Vec<i64> = (0..row_count as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(col_data.as_ptr() as *const u8, col_data.len() * 8)
        };
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            id: 0,
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            row_count,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: false,
            not_null_hint: false,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };
        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let (schema, _empty_meta) = to_parquet_schema(&partition, false, -1, -1).unwrap();
        let encodings = to_encodings(&partition);
        let compressions = to_compressions(&partition);

        let mut parquet_buf = Vec::new();
        let mut chunked = ParquetWriter::new(&mut parquet_buf)
            .with_statistics(false)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(rows_per_group))
            .chunked_with_compressions(schema, encodings, compressions)
            .unwrap();
        chunked.write_chunk(&partition).unwrap();

        let mut qdb_meta = QdbMeta::new(1);
        qdb_meta.schema.push(QdbMetaCol {
            column_type: ColumnTypeTag::Timestamp
                .into_type()
                .into_designated()
                .unwrap(),
            column_top: 0,
            format: None,
            ascii: None,
        });
        let qdb_meta_json = qdb_meta.serialize().unwrap();
        chunked
            .finish(vec![KeyValue {
                key: QDB_META_KEY.to_string(),
                value: Some(qdb_meta_json),
            }])
            .unwrap();
        (parquet_buf, qdb_meta)
    }

    /// Regenerates the committed test fixture consumed by
    /// `Mig941Test#testMigrateBackfillsMissingTsStats`. Run with
    /// `cargo test emit_mig941_ts_no_stats_fixture -- --ignored` after
    /// changing the parquet write path in a way that affects the fixture.
    #[test]
    #[ignore]
    fn emit_mig941_ts_no_stats_fixture() {
        let (bytes, _qdb_meta) = write_parquet_without_ts_stats(20, 10);
        let out = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../src/test/resources/mig941/ts_no_stats.parquet");
        std::fs::create_dir_all(out.parent().unwrap()).unwrap();
        std::fs::write(&out, &bytes).unwrap();
        eprintln!("wrote {} bytes to {}", bytes.len(), out.display());
    }

    #[test]
    fn backfill_fills_stat_flags_and_values() {
        let (parquet_bytes, qdb_meta) = write_parquet_without_ts_stats(10, 10);
        let mut cursor = Cursor::new(&parquet_bytes);
        let metadata = read_metadata_with_size(&mut cursor, parquet_bytes.len() as u64).unwrap();

        let backfill = |_rg: usize, _lo: usize, _hi: usize| -> ParquetResult<i64> { Ok(42) };
        let (parquet_meta_bytes, parquet_meta_file_size) =
            convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, Some(&backfill)).unwrap();
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        let chunk = reader.row_group(0).unwrap().column_chunk(0).unwrap();
        let flags = StatFlags(chunk.stat_flags);
        assert!(flags.has_min_stat() && flags.is_min_inlined());
        assert!(flags.has_max_stat() && flags.is_max_inlined());
        assert_eq!(chunk.min_stat as i64, 42);
        assert_eq!(chunk.max_stat as i64, 42);
    }

    #[test]
    fn backfill_skipped_when_inline_stats_already_present() {
        let parquet_bytes = write_test_parquet(10, CompressionOptions::Uncompressed);
        let mut cursor = Cursor::new(&parquet_bytes);
        let metadata = read_metadata_with_size(&mut cursor, parquet_bytes.len() as u64).unwrap();
        let qdb_meta = metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == crate::parquet::qdb_metadata::QDB_META_KEY)
                    .and_then(|kv| kv.value.as_deref())
            })
            .map(|json| QdbMeta::deserialize(json).unwrap());

        let backfill = |_rg: usize, _lo: usize, _hi: usize| -> ParquetMetaResult<i64> {
            panic!("backfill must not be called when inline stats exist");
        };
        let (_parquet_meta_bytes, _parquet_meta_file_size) =
            convert_from_parquet(&metadata, qdb_meta.as_ref(), 0, 0, Some(&backfill)).unwrap();
    }
}
