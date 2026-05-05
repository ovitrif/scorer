use bitcoin::io;
use lightning::ln::msgs::DecodeError;
use lightning::types::payment::{PaymentHash, PaymentPreimage, PaymentSecret};
use lightning::util::hash_tables::HashMap;
use lightning::util::ser::{Readable, Writeable, Writer};
use lightning::{impl_writeable_tlv_based, impl_writeable_tlv_based_enum};
use std::fmt;

#[derive(Copy, Clone)]
pub(crate) enum HTLCStatus {
	Pending,
	Succeeded,
	Failed,
}

impl_writeable_tlv_based_enum!(HTLCStatus,
	(0, Pending) => {},
	(1, Succeeded) => {},
	(2, Failed) => {},
);

pub(crate) struct MillisatAmount(pub(crate) Option<u64>);

impl fmt::Display for MillisatAmount {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.0 {
			Some(amt) => write!(f, "{}", amt),
			None => write!(f, "unknown"),
		}
	}
}

impl Readable for MillisatAmount {
	fn read<R: io::Read>(r: &mut R) -> Result<Self, DecodeError> {
		let amt: Option<u64> = Readable::read(r)?;
		Ok(MillisatAmount(amt))
	}
}

impl Writeable for MillisatAmount {
	fn write<W: Writer>(&self, w: &mut W) -> Result<(), io::Error> {
		self.0.write(w)
	}
}

pub(crate) struct PaymentInfo {
	pub(crate) preimage: Option<PaymentPreimage>,
	pub(crate) secret: Option<PaymentSecret>,
	pub(crate) status: HTLCStatus,
	pub(crate) amt_msat: MillisatAmount,
}

impl_writeable_tlv_based!(PaymentInfo, {
	(0, preimage, required),
	(2, secret, required),
	(4, status, required),
	(6, amt_msat, required),
});

pub(crate) struct InboundPaymentInfoStorage {
	pub(crate) payments: HashMap<PaymentHash, PaymentInfo>,
}

impl_writeable_tlv_based!(InboundPaymentInfoStorage, {
	(0, payments, required),
});

pub(crate) struct OutboundPaymentInfoStorage {
	pub(crate) payments: HashMap<lightning::ln::channelmanager::PaymentId, PaymentInfo>,
}

impl_writeable_tlv_based!(OutboundPaymentInfoStorage, {
	(0, payments, required),
});
