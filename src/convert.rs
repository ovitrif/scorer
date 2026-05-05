use bitcoin::{Address, BlockHash, Txid};
use lightning_block_sync::http::JsonResponse;
use serde_json::Value;
use std::convert::TryInto;
use std::io::{Error, ErrorKind};
use std::str::FromStr;

fn invalid_field(field: &str, message: &str) -> Error {
	Error::new(
		ErrorKind::InvalidData,
		format!("invalid or missing field `{}` in bitcoind response: {}", field, message),
	)
}

fn as_str_field<'a>(json: &'a Value, field: &str) -> Result<&'a str, Error> {
	json[field].as_str().ok_or_else(|| invalid_field(field, "expected string"))
}

fn as_u64_field(json: &Value, field: &str) -> Result<u64, Error> {
	json[field].as_u64().ok_or_else(|| invalid_field(field, "expected unsigned integer"))
}

fn as_i64_field(json: &Value, field: &str) -> Result<i64, Error> {
	json[field].as_i64().ok_or_else(|| invalid_field(field, "expected signed integer"))
}

fn as_bool_field(json: &Value, field: &str) -> Result<bool, Error> {
	json[field].as_bool().ok_or_else(|| invalid_field(field, "expected boolean"))
}

fn as_f64_field(json: &Value, field: &str) -> Result<f64, Error> {
	json[field].as_f64().ok_or_else(|| invalid_field(field, "expected number"))
}

pub struct FundedTx {
	pub changepos: i64,
	pub hex: String,
}

impl TryInto<FundedTx> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> std::io::Result<FundedTx> {
		Ok(FundedTx {
			changepos: as_i64_field(&self.0, "changepos")?,
			hex: as_str_field(&self.0, "hex")?.to_string(),
		})
	}
}

pub struct RawTx(pub String);

impl TryInto<RawTx> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> std::io::Result<RawTx> {
		Ok(RawTx(
			self.0
				.as_str()
				.ok_or_else(|| invalid_field("<root>", "expected raw transaction hex string"))?
				.to_string(),
		))
	}
}

pub struct SignedTx {
	pub complete: bool,
	pub hex: String,
}

impl TryInto<SignedTx> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> std::io::Result<SignedTx> {
		Ok(SignedTx {
			hex: as_str_field(&self.0, "hex")?.to_string(),
			complete: as_bool_field(&self.0, "complete")?,
		})
	}
}

pub struct NewAddress(pub String);
impl TryInto<NewAddress> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> std::io::Result<NewAddress> {
		Ok(NewAddress(
			self.0
				.as_str()
				.ok_or_else(|| invalid_field("<root>", "expected address string"))?
				.to_string(),
		))
	}
}

pub struct FeeResponse {
	pub feerate_sat_per_kw: Option<u32>,
	pub errored: bool,
}

impl TryInto<FeeResponse> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> std::io::Result<FeeResponse> {
		let errored = !self.0["errors"].is_null();
		Ok(FeeResponse {
			errored,
			// Bitcoin Core gives us a feerate in BTC/KvB, which we need to convert to
			// satoshis/KW. Thus, we first multiply by 10^8 to get satoshis, then divide by 4
			// to convert virtual-bytes into weight units.
			feerate_sat_per_kw: self.0["feerate"].as_f64().map(|feerate_btc_per_kvbyte| {
				(feerate_btc_per_kvbyte * 100_000_000.0 / 4.0).round() as u32
			}),
		})
	}
}

pub struct MempoolMinFeeResponse {
	pub feerate_sat_per_kw: Option<u32>,
	pub errored: bool,
}

impl TryInto<MempoolMinFeeResponse> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> std::io::Result<MempoolMinFeeResponse> {
		let errored = !self.0["errors"].is_null();
		Ok(MempoolMinFeeResponse {
			errored,
			// Bitcoin Core gives us a feerate in BTC/KvB, which we need to convert to
			// satoshis/KW. Thus, we first multiply by 10^8 to get satoshis, then divide by 4
			// to convert virtual-bytes into weight units.
			feerate_sat_per_kw: self.0["mempoolminfee"].as_f64().map(|feerate_btc_per_kvbyte| {
				(feerate_btc_per_kvbyte * 100_000_000.0 / 4.0).round() as u32
			}),
		})
	}
}

pub struct BlockchainInfo {
	pub latest_height: usize,
	pub latest_blockhash: BlockHash,
	pub chain: String,
}

impl TryInto<BlockchainInfo> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> std::io::Result<BlockchainInfo> {
		Ok(BlockchainInfo {
			latest_height: as_u64_field(&self.0, "blocks")? as usize,
			latest_blockhash: BlockHash::from_str(as_str_field(&self.0, "bestblockhash")?)
				.map_err(|e| {
					Error::new(
						ErrorKind::InvalidData,
						format!("invalid field `bestblockhash` in bitcoind response: {}", e),
					)
				})?,
			chain: as_str_field(&self.0, "chain")?.to_string(),
		})
	}
}

pub struct ListUnspentUtxo {
	pub txid: Txid,
	pub vout: u32,
	pub amount: u64,
	pub address: Address,
}

pub struct ListUnspentResponse(pub Vec<ListUnspentUtxo>);

impl TryInto<ListUnspentResponse> for JsonResponse {
	type Error = std::io::Error;
	fn try_into(self) -> Result<ListUnspentResponse, Self::Error> {
		let utxo_entries =
			self.0.as_array().ok_or_else(|| invalid_field("<root>", "expected array of UTXOs"))?;
		let mut utxos = Vec::with_capacity(utxo_entries.len());
		for utxo in utxo_entries {
			let txid = Txid::from_str(as_str_field(utxo, "txid")?).map_err(|e| {
				Error::new(
					ErrorKind::InvalidData,
					format!("invalid field `txid` in listunspent response: {}", e),
				)
			})?;
			let vout = as_u64_field(utxo, "vout")? as u32;
			let amount = bitcoin::Amount::from_btc(as_f64_field(utxo, "amount")?)
				.map_err(|e| {
					Error::new(
						ErrorKind::InvalidData,
						format!("invalid field `amount` in listunspent response: {}", e),
					)
				})?
				.to_sat();
			let address = Address::from_str(as_str_field(utxo, "address")?)
				.map_err(|e| {
					Error::new(
						ErrorKind::InvalidData,
						format!("invalid field `address` in listunspent response: {}", e),
					)
				})?
				.assume_checked();
			utxos.push(ListUnspentUtxo { txid, vout, amount, address });
		}
		Ok(ListUnspentResponse(utxos))
	}
}
