//! `sendpayment` / `keysend` / `getinvoice` helpers.

use std::time::Duration;

use bitcoin::hashes::{sha256::Hash as Sha256, Hash};
use bitcoin::secp256k1::PublicKey;
use lightning::ln::channelmanager::{
	Bolt11InvoiceParameters, PaymentId, RecipientOnionFields, Retry,
};
use lightning::routing::router::{PaymentParameters, RouteParameters, RouteParametersConfig};
use lightning::sign::EntropySource;
use lightning::types::payment::{PaymentHash, PaymentPreimage};
use lightning::util::logger::Logger;
use lightning::util::persist::KVStore;
use lightning::util::ser::Writeable;
use lightning_invoice::Bolt11Invoice;
use lightning_persister::fs_store::FilesystemStore;
use tokio::sync::Mutex as AsyncMutex;

use crate::disk::OUTBOUND_PAYMENTS_FNAME;
use crate::{
	prompt, user_err, user_out, ChannelManager, FilesystemLogger, HTLCStatus,
	InboundPaymentInfoStorage, MillisatAmount, OutboundPaymentInfoStorage, PaymentInfo,
};

pub(crate) async fn send_payment(
	channel_manager: &ChannelManager, invoice: &Bolt11Invoice, required_amount_msat: Option<u64>,
	outbound_payments: &AsyncMutex<OutboundPaymentInfoStorage>, fs_store: &FilesystemStore,
	logger: &FilesystemLogger,
) {
	let payment_id = PaymentId((*invoice.payment_hash()).to_byte_array());
	let payment_secret = Some(*invoice.payment_secret());
	let amt_msat = match (invoice.amount_milli_satoshis(), required_amount_msat) {
		// pay_for_bolt11_invoice only validates that the amount we pay is >= the invoice's
		// required amount, not that its equal (to allow for overpayment). As that is somewhat
		// surprising, here we check and reject all disagreements in amount.
		(Some(inv_amt), Some(req_amt)) if inv_amt != req_amt => {
			user_err!(
				logger,
				"Amount didn't match invoice value of {}msat",
				invoice.amount_milli_satoshis().unwrap_or(0)
			);
			prompt!();
			return;
		},
		(Some(inv_amt), _) => inv_amt,
		(_, Some(req_amt)) => req_amt,
		(None, None) => {
			user_err!(logger, "Need an amount to pay an amountless invoice");
			prompt!();
			return;
		},
	};
	let write_future = {
		let mut outbound_payments = outbound_payments.lock().await;
		outbound_payments.payments.insert(
			payment_id,
			PaymentInfo {
				preimage: None,
				secret: payment_secret,
				status: HTLCStatus::Pending,
				amt_msat: MillisatAmount(Some(amt_msat)),
			},
		);
		fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
	};
	if let Err(e) = write_future.await {
		user_err!(logger, "failed to persist outbound payments: {}", e);
		prompt!();
		return;
	}

	match channel_manager.pay_for_bolt11_invoice(
		invoice,
		payment_id,
		required_amount_msat,
		RouteParametersConfig::default(),
		Retry::Timeout(Duration::from_secs(10)),
	) {
		Ok(_) => {
			let payee_pubkey = invoice.recover_payee_pub_key();
			user_out!(logger, "EVENT: initiated sending {} msats to {}", amt_msat, payee_pubkey);
			prompt!();
		},
		Err(e) => {
			user_err!(logger, "failed to send payment: {:?}", e);
			prompt!();
			let write_future = {
				let mut outbound_payments = outbound_payments.lock().await;
				if let Some(payment) = outbound_payments.payments.get_mut(&payment_id) {
					payment.status = HTLCStatus::Failed;
				}
				fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
			};
			if let Err(write_err) = write_future.await {
				user_err!(logger, "failed to persist outbound payments: {}", write_err);
			}
		},
	};
}

pub(crate) async fn keysend<E: EntropySource>(
	channel_manager: &ChannelManager, payee_pubkey: PublicKey, amt_msat: u64, entropy_source: &E,
	outbound_payments: &AsyncMutex<OutboundPaymentInfoStorage>, fs_store: &FilesystemStore,
	logger: &FilesystemLogger,
) {
	let payment_preimage = PaymentPreimage(entropy_source.get_secure_random_bytes());
	let payment_id = PaymentId(Sha256::hash(&payment_preimage.0[..]).to_byte_array());

	let route_params = RouteParameters::from_payment_params_and_value(
		PaymentParameters::for_keysend(payee_pubkey, 40, false),
		amt_msat,
	);
	let write_future = {
		let mut outbound_payments = outbound_payments.lock().await;
		outbound_payments.payments.insert(
			payment_id,
			PaymentInfo {
				preimage: None,
				secret: None,
				status: HTLCStatus::Pending,
				amt_msat: MillisatAmount(Some(amt_msat)),
			},
		);
		fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
	};
	if let Err(e) = write_future.await {
		user_err!(logger, "failed to persist outbound payments: {}", e);
		prompt!();
		return;
	}
	match channel_manager.send_spontaneous_payment(
		Some(payment_preimage),
		RecipientOnionFields::spontaneous_empty(),
		payment_id,
		route_params,
		Retry::Timeout(Duration::from_secs(10)),
	) {
		Ok(_payment_hash) => {
			user_out!(logger, "EVENT: initiated sending {} msats to {}", amt_msat, payee_pubkey);
			prompt!();
		},
		Err(e) => {
			user_err!(logger, "failed to send payment: {:?}", e);
			prompt!();
			let write_future = {
				let mut outbound_payments = outbound_payments.lock().await;
				if let Some(payment) = outbound_payments.payments.get_mut(&payment_id) {
					payment.status = HTLCStatus::Failed;
				}
				fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
			};
			if let Err(write_err) = write_future.await {
				user_err!(logger, "failed to persist outbound payments: {}", write_err);
			}
		},
	};
}

pub(crate) fn get_invoice(
	amt_msat: u64, inbound_payments: &mut InboundPaymentInfoStorage,
	channel_manager: &ChannelManager, expiry_secs: u32, logger: &FilesystemLogger,
) {
	let invoice_params = Bolt11InvoiceParameters {
		amount_msats: Some(amt_msat),
		invoice_expiry_delta_secs: Some(expiry_secs),
		..Default::default()
	};
	let invoice = match channel_manager.create_bolt11_invoice(invoice_params) {
		Ok(inv) => {
			user_out!(logger, "SUCCESS: generated invoice: {}", inv);
			inv
		},
		Err(e) => {
			user_err!(logger, "failed to create invoice: {:?}", e);
			return;
		},
	};

	let payment_hash = PaymentHash(invoice.payment_hash().to_byte_array());
	inbound_payments.payments.insert(
		payment_hash,
		PaymentInfo {
			preimage: None,
			secret: Some(*invoice.payment_secret()),
			status: HTLCStatus::Pending,
			amt_msat: MillisatAmount(Some(amt_msat)),
		},
	);
}


