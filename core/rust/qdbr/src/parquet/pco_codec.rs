//! Safe wrappers around the pco (pcodec) numeric codec.
//!
//! pco is a fitted, lossless codec for sequences of numbers. QuestDB uses it as
//! the default back end for the lossy Parquet float path (after Tier A mantissa
//! rounding) and, optionally, as a lossless numeric codec. The blob it produces
//! is NOT a standard Parquet encoding, so a column stored this way is readable
//! only by QuestDB; the page carries a `QdbMetaColFormat::PcoEncoded` marker.
//!
//! Every public function returns a `ParquetResult` rather than panicking: a pco
//! error on malformed input becomes an `InvalidLayout` error. This matters
//! because these functions run behind JNI, where a panic aborts the JVM.

use pco::data_types::Number;
use pco::standalone::{simple_compress, simple_decompress};
use pco::ChunkConfig;

use crate::parquet::error::{fmt_err, ParquetResult};

/// pco compression level. Level 8 is pco's default and the ratio/speed sweet
/// spot in the float-compression benchmark; higher levels add little.
pub const PCO_COMPRESSION_LEVEL: usize = 8;

/// Compress a slice of numbers into a self-describing pco blob.
pub fn compress<T: Number>(values: &[T]) -> ParquetResult<Vec<u8>> {
    let config = ChunkConfig::default().with_compression_level(PCO_COMPRESSION_LEVEL);
    simple_compress(values, &config).map_err(|e| fmt_err!(Unsupported, "pco compress failed: {e}"))
}

/// Decompress a pco blob produced by [`compress`] back into a vector of numbers.
///
/// Returns an error (never panics) when `src` is not a valid pco blob.
pub fn decompress<T: Number>(src: &[u8]) -> ParquetResult<Vec<T>> {
    simple_decompress(src).map_err(|e| fmt_err!(InvalidLayout, "pco decompress failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_f64_exactly() {
        let values: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.5 - 2_500.0).collect();
        let blob = compress(&values).unwrap();
        let decoded: Vec<f64> = decompress(&blob).unwrap();
        assert_eq!(values, decoded);
        assert!(blob.len() < values.len() * 8, "pco should shrink a ramp");
    }

    #[test]
    fn round_trips_f32_exactly() {
        let values: Vec<f32> = (0..10_000).map(|i| (i as f32 * 0.01).sin()).collect();
        let blob = compress(&values).unwrap();
        let decoded: Vec<f32> = decompress(&blob).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn preserves_non_finite_and_zero() {
        // NaN is the QuestDB null sentinel; it must survive the round trip.
        let values: Vec<f64> = vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0,
            -0.0,
            1.5,
            -1.5,
        ];
        let blob = compress(&values).unwrap();
        let decoded: Vec<f64> = decompress(&blob).unwrap();
        assert!(decoded[0].is_nan());
        assert_eq!(decoded[1], f64::INFINITY);
        assert_eq!(decoded[2], f64::NEG_INFINITY);
        assert_eq!(decoded[3], 0.0);
        assert_eq!(decoded[4], -0.0);
        assert_eq!(decoded[5], 1.5);
        assert_eq!(decoded[6], -1.5);
    }

    #[test]
    fn round_trips_empty() {
        let values: Vec<f64> = Vec::new();
        let blob = compress(&values).unwrap();
        let decoded: Vec<f64> = decompress(&blob).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn corrupt_input_errors_without_panicking() {
        let garbage = [0u8, 1, 2, 3, 4, 5, 6, 7, 255, 254, 253, 0, 0, 0];
        let result: ParquetResult<Vec<f64>> = decompress(&garbage);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_blob_errors_without_panicking() {
        let values: Vec<f64> = (0..1_000).map(|i| i as f64).collect();
        let blob = compress(&values).unwrap();
        let result: ParquetResult<Vec<f64>> = decompress(&blob[..blob.len() / 2]);
        assert!(result.is_err());
    }
}
