//! Master-key sealing for connection secrets (ChaCha20-Poly1305).

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use zeroize::{Zeroize, ZeroizeOnDrop};

use super::error::{StoreError, StoreResult};
use super::types::SealedSecret;

/// Environment variable that holds the store key as 64 hex characters.
pub const MASTER_KEY_ENV: &str = "DIAVASI_STORE_KEY";
pub const KEY_LEN: usize = 32;
pub const NONCE_LEN: usize = 12;

/// 32-byte master key. Zeroized on drop.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct StoreKey([u8; KEY_LEN]);

impl StoreKey {
    /// A key from raw bytes.
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    /// A random key. Secrets sealed with it are unreadable once the process exits, unless the key is saved.
    pub fn generate() -> Self {
        let mut bytes = [0u8; KEY_LEN];
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Parse the key from `DIAVASI_STORE_KEY` (64 hex characters). `Ok(None)`
    /// when the variable is unset or empty.
    pub fn from_env() -> StoreResult<Option<Self>> {
        Self::from_env_value(std::env::var(MASTER_KEY_ENV).ok().as_deref())
    }

    /// [`Self::from_env`] without reading the process environment.
    pub fn from_env_value(value: Option<&str>) -> StoreResult<Option<Self>> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            Some(hex_key) => Self::from_hex(hex_key).map(Some),
            None => Ok(None),
        }
    }

    /// A key from 64 hex characters. Surrounding whitespace is ignored.
    ///
    /// ```
    /// use diavasi::store::{StoreKey, open_secret, seal_secret};
    /// let key = StoreKey::from_hex(&"00".repeat(32))?;
    /// let sealed = seal_secret(&key, b"s3cret")?;
    /// assert_eq!(open_secret(&key, &sealed)?, b"s3cret");
    /// assert!(open_secret(&StoreKey::generate(), &sealed).is_err());
    /// # Ok::<(), diavasi::store::StoreError>(())
    /// ```
    pub fn from_hex(hex_key: &str) -> StoreResult<Self> {
        let bytes = hex::decode(hex_key.trim())
            .map_err(|e| StoreError::Crypto(format!("invalid hex key: {e}")))?;
        if bytes.len() != KEY_LEN {
            return Err(StoreError::Crypto(format!(
                "key must be {KEY_LEN} bytes ({} hex chars), got {} bytes",
                KEY_LEN * 2,
                bytes.len()
            )));
        }
        let mut arr = [0u8; KEY_LEN];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// The raw key bytes.
    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// The key as 64 hex characters, the form `DIAVASI_STORE_KEY` takes.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

/// Seal plaintext secret bytes under the master key.
pub fn seal_secret(key: &StoreKey, plaintext: &[u8]) -> StoreResult<SealedSecret> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key.as_bytes()));
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| StoreError::Crypto(format!("seal failed: {e}")))?;
    Ok(SealedSecret {
        nonce: nonce_bytes.to_vec(),
        ciphertext,
    })
}

/// Open a sealed secret. For adapter use later; list/show paths must not call this.
pub fn open_secret(key: &StoreKey, sealed: &SealedSecret) -> StoreResult<Vec<u8>> {
    if sealed.nonce.len() != NONCE_LEN {
        return Err(StoreError::Crypto(format!(
            "nonce must be {NONCE_LEN} bytes"
        )));
    }
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key.as_bytes()));
    let nonce = Nonce::from_slice(&sealed.nonce);
    cipher
        .decrypt(nonce, sealed.ciphertext.as_ref())
        .map_err(|e| StoreError::Crypto(format!("open failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let key = StoreKey::generate();
        let sealed = seal_secret(&key, b"hunter2").unwrap();
        assert_ne!(sealed.ciphertext, b"hunter2");
        let plain = open_secret(&key, &sealed).unwrap();
        assert_eq!(plain, b"hunter2");
    }

    #[test]
    fn hex_key() {
        let key = StoreKey::generate();
        let hex = key.to_hex();
        let again = StoreKey::from_hex(&hex).unwrap();
        assert_eq!(key.as_bytes(), again.as_bytes());
    }

    #[test]
    fn from_bytes_and_empty_plaintext() {
        let key = StoreKey::from_bytes([7u8; KEY_LEN]);
        let sealed = seal_secret(&key, b"").unwrap();
        assert!(open_secret(&key, &sealed).unwrap().is_empty());
    }

    #[test]
    fn from_hex_rejects_bad_input() {
        assert!(StoreKey::from_hex("not-hex!!").is_err());
        assert!(StoreKey::from_hex("abcd").is_err()); // wrong length
        let padded = format!("  {}  ", StoreKey::generate().to_hex());
        assert!(StoreKey::from_hex(&padded).is_ok());
    }

    #[test]
    fn open_rejects_bad_nonce_and_wrong_key() {
        let key = StoreKey::generate();
        let sealed = seal_secret(&key, b"secret").unwrap();
        let bad_nonce = SealedSecret {
            nonce: vec![0u8; 4],
            ciphertext: sealed.ciphertext.clone(),
        };
        assert!(open_secret(&key, &bad_nonce).is_err());

        let other = StoreKey::generate();
        assert!(open_secret(&other, &sealed).is_err());

        let mut tampered = sealed.clone();
        if let Some(b) = tampered.ciphertext.last_mut() {
            *b ^= 0xff;
        }
        assert!(open_secret(&key, &tampered).is_err());
    }

    #[test]
    fn from_env_value_parses_or_reports_absence() {
        let key = StoreKey::generate();
        let loaded = StoreKey::from_env_value(Some(&key.to_hex()))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.as_bytes(), key.as_bytes());
        assert!(StoreKey::from_env_value(None).unwrap().is_none());
        assert!(StoreKey::from_env_value(Some("  ")).unwrap().is_none());
        assert!(StoreKey::from_env_value(Some("abcd")).is_err());
    }
}
