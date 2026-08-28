pub mod ahp_store;
pub mod config;
pub mod db;
pub mod ipc;
pub mod mcp;
pub mod protocol;
pub mod qq;
pub mod security;
pub mod service;

pub const IPC_PROTOCOL_VERSION: u32 = 1;
pub const MAX_IPC_MESSAGE_BYTES: usize = 1024 * 1024;
