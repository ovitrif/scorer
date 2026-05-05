use crate::runtime_config::LdkUserInfo;
use std::env;
use std::fs;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum StartupArgsError {
	#[error("missing storage directory argument")]
	MissingStorageDirectory,
	#[error("failed to create LDK data directory {path}: {source}")]
	CreateDataDir {
		path: String,
		#[source]
		source: std::io::Error,
	},
	#[error(transparent)]
	Config(#[from] crate::config::ConfigError),
}

pub(crate) fn parse_startup_args() -> Result<LdkUserInfo, StartupArgsError> {
	let args: Vec<String> = env::args().collect();
	if args.len() < 2 {
		return Err(StartupArgsError::MissingStorageDirectory);
	}

	let ldk_storage_dir_path = args[1].clone();
	let ldk_data_dir = format!("{}/.ldk", ldk_storage_dir_path);

	if let Err(source) = fs::create_dir_all(&ldk_data_dir) {
		return Err(StartupArgsError::CreateDataDir { path: ldk_data_dir, source });
	}

	let config = crate::config::NodeConfig::load(&ldk_data_dir)?;
	Ok(config.into_ldk_user_info(ldk_storage_dir_path))
}
