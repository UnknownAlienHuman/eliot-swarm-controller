//! Shared Concilium wire bounds for Store validation and frontend schemas.
//! Text bounds count UTF-8 bytes; JSON Schema length constraints are also
//! described as byte bounds by the frontend and checked by the wire parser.

pub const MAX_CONCILIUM_REQUEST_BYTES: usize = 32 * 1024;
pub const MAX_CLAIMS_PER_POSITION: usize = 64;
pub const MAX_EVIDENCE_REFS: usize = 8;
pub const MAX_CLIENT_REQUEST_ID_BYTES: usize = 128;
pub const MAX_CLIENT_ID_BYTES: usize = 128;
pub const MAX_IDENTIFIER_BYTES: usize = 256;
pub const MAX_PARTICIPANT_REASON_BYTES: usize = 1024;
pub const MAX_QUESTION_BYTES: usize = 2048;
pub const MAX_CONFLICT_BYTES: usize = 4096;
pub const MAX_CLAIM_TEXT_BYTES: usize = 4096;
pub const MAX_POSITION_TEXT_BYTES: usize = 4096;
pub const MAX_EVIDENCE_REF_BYTES: usize = 512;
pub const MAX_READ_PAGE_SIZE: i64 = 50;
pub const DEFAULT_READ_PAGE_SIZE: i64 = 20;
