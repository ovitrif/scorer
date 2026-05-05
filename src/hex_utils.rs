use bitcoin::secp256k1::PublicKey;
use std::fmt::Write;

pub fn to_vec(hex: &str) -> Option<Vec<u8>> {
	if (hex.len() & 1) != 0 {
		return None;
	}

	let mut out = Vec::with_capacity(hex.len() / 2);

	let mut b = 0;
	for (idx, c) in hex.as_bytes().iter().enumerate() {
		b <<= 4;
		match *c {
			b'A'..=b'F' => b |= c - b'A' + 10,
			b'a'..=b'f' => b |= c - b'a' + 10,
			b'0'..=b'9' => b |= c - b'0',
			_ => return None,
		}
		if (idx & 1) == 1 {
			out.push(b);
			b = 0;
		}
	}

	Some(out)
}

#[inline]
pub fn hex_str(value: &[u8]) -> String {
	let mut res = String::with_capacity(2 * value.len());
	for v in value {
		write!(&mut res, "{:02x}", v).expect("Unable to write");
	}
	res
}

pub fn to_compressed_pubkey(hex: &str) -> Option<PublicKey> {
	if hex.len() != 33 * 2 {
		return None;
	}
	let data = to_vec(&hex[0..33 * 2])?;
	PublicKey::from_slice(&data).ok()
}

#[cfg(test)]
mod tests {
	#[test]
	fn to_vec_rejects_odd_length() {
		assert!(super::to_vec("abc").is_none());
	}

	#[test]
	fn to_vec_decodes_even_length_hex() {
		assert_eq!(super::to_vec("0aBc"), Some(vec![0x0a, 0xbc]));
	}
}
