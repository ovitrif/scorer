//! Low-level networking helpers shared between the REPL and event-handler.
//!
//! Historically, `events.rs` called into `cli.rs` for `connect_peer_if_necessary`.
//! That edge is cyclic in spirit (event handling and REPL are peers, neither
//! should own the other). This module hosts the neutral primitives both
//! layers depend on.

pub(crate) mod peer;
