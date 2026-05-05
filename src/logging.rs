//! Dual-output logging macros.
//!
//! ldk-sample exposes an interactive REPL on stdout AND persists a
//! rotating log file via [`FilesystemLogger`](crate::disk::FilesystemLogger).
//! Historically these were two disjoint channels: `println!` went to the
//! user's terminal but *not* the log file, and `lightning::log_*!` wrote
//! to the log file but *not* the terminal. Anyone running the node as a
//! daemon therefore lost all of the "ERROR:" messages that the REPL
//! surfaces.
//!
//! This module unifies them. Every error-class message should use
//! [`user_err!`] / [`user_warn!`]; every success/result message should
//! use [`user_out!`]. All three write to the terminal *and* the log file
//! with a matching severity.
//!
//! `prompt!` redraws the `> ` prompt without logging — the prompt is
//! terminal chrome, not a log-worthy event.
//!
//! ## Why macros (not functions)
//!
//! `lightning::log_*!` are themselves macros that capture `module_path!`
//! and `line!` from their invocation site; wrapping them in a function
//! would collapse every log entry's origin to `src/logging.rs`, which is
//! worse than useless. Macros preserve the caller's source location.

/// Flush stdout, swallowing the (exceptionally rare) error. Used internally
/// by the macros below; exposed for the `prompt!` macro.
///
/// `std::io::Write` is imported inside the function so callers of
/// `prompt!()` don't need to remember to bring the trait into scope.
pub(crate) fn flush_stdout_silently() {
	use std::io::Write;
	let _ = std::io::stdout().flush();
}

/// Write a success/result line to the terminal AND the log file at INFO
/// level. The first argument is an `&impl Logger` (typically
/// `&*logger` where `logger: Arc<FilesystemLogger>`), followed by a
/// format string and arguments as in `println!`.
///
/// ```ignore
/// user_out!(logger, "Connected to peer {}", pubkey);
/// ```
#[macro_export]
macro_rules! user_out {
	($logger:expr, $($arg:tt)*) => {{
		let __msg = format!($($arg)*);
		println!("{}", __msg);
		lightning::log_info!($logger, "{}", __msg);
	}};
}

/// Write a warning line to the terminal AND the log file at WARN level.
#[macro_export]
macro_rules! user_warn {
	($logger:expr, $($arg:tt)*) => {{
		let __msg = format!($($arg)*);
		println!("WARN: {}", __msg);
		lightning::log_warn!($logger, "{}", __msg);
	}};
}

/// Write an error line to the terminal AND the log file at ERROR level.
///
/// After writing, the caller is expected to redraw the prompt via
/// [`prompt!`] if they are inside the REPL loop. This separation lets
/// event-handling code (which is not tied to the prompt) emit errors
/// without interleaving `> ` into the output.
#[macro_export]
macro_rules! user_err {
	($logger:expr, $($arg:tt)*) => {{
		let __msg = format!($($arg)*);
		// The leading newline matches the historical REPL UX, where
		// errors arrive out of band and should visually separate from
		// in-progress output.
		println!("\nERROR: {}", __msg);
		lightning::log_error!($logger, "{}", __msg);
	}};
}

/// Redraw the `> ` REPL prompt without logging. Purely terminal chrome.
#[macro_export]
macro_rules! prompt {
	() => {{
		print!("> ");
		$crate::logging::flush_stdout_silently();
	}};
}
