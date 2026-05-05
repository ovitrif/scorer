//! Command-helper submodules called from the REPL dispatcher in
//! `cli::poll_for_user_input`.
//!
//! Each submodule owns one command family and exposes a small number of
//! `pub(super)` or `pub(crate)` helpers. Factoring out of the 1,278-line
//! `cli.rs` means each command's logic and error handling is reviewable
//! in isolation.

pub(super) mod channel;
pub(super) mod payment;
pub(super) mod peer;
