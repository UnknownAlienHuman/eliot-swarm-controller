//! Shared bounds for Thread, peer contracts, and integration acknowledgements.
//! Text bounds count UTF-8 bytes. Frontend schemas describe the byte bound;
//! admission validates it before recording a mutation.
//! Member counts are bounded by the request size, without a separate roster cap.

pub const MAX_COORDINATION_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_CLIENT_REQUEST_ID_BYTES: usize = 128;
pub const MAX_CLIENT_ID_BYTES: usize = 128;
pub const MAX_IDENTIFIER_BYTES: usize = 256;
pub const MAX_REFERENCE_BYTES: usize = 512;
pub const MAX_SUBJECT_BYTES: usize = 1024;
pub const MAX_SUMMARY_BYTES: usize = 4096;
pub const MAX_INLINE_BODY_BYTES: usize = 16 * 1024;
pub const MAX_REASON_BYTES: usize = 4096;
pub const MAX_EVIDENCE_REFS: usize = 8;
pub const MAX_READ_PAGE_SIZE: i64 = 50;
pub const DEFAULT_READ_PAGE_SIZE: i64 = 20;
