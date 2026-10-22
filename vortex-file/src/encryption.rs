// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Segment-level encryption of Vortex files.
//!
//! Encryption operates on whole segments: every data segment, and the dtype, layout, statistics,
//! footer, and user metadata segments referenced by the postscript, can be encrypted with
//! AES-256-GCM. The postscript itself stays in plaintext, since it only records segment locations
//! and the [`EncryptionSpec`] each segment was encrypted with.
//!
//! Keys are never written to the file. Writers configure an [`EncryptionConfig`], whose opaque
//! key metadata is stored in the file so that readers can resolve the key again through a
//! [`KeyRetriever`].
//!
//! An encrypted segment is stored as `ciphertext || nonce || tag`. The ciphertext has the length
//! of the plaintext, so it starts at the segment's aligned offset. The additional authenticated
//! data binds the ciphertext to its file, through random per-file bytes, and to its position in
//! the file, so that segments cannot be swapped without failing decryption. An optional AAD prefix
//! additionally binds the file to an application-defined identity, such as its path, so that a
//! file cannot be replaced by another file encrypted with the same key. The prefix is not stored
//! in the file, so readers must supply it.

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use aes_gcm::AeadInOut;
use aes_gcm::Aes256Gcm;
use aes_gcm::KeyInit;
use aes_gcm::Nonce;
use aes_gcm::Tag;
use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use zeroize::Zeroize;

use crate::flatbuffers::footer as fb;

/// Length in bytes of an AES-256 key.
pub const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const AAD_FILE_UNIQUE_LEN: usize = 16;

/// Number of bytes an encrypted segment occupies in addition to its plaintext.
pub const ENCRYPTION_OVERHEAD: usize = NONCE_LEN + TAG_LEN;

/// A 256-bit key used to encrypt and decrypt Vortex file segments.
///
/// The key bytes are zeroed on drop and are not printed by [`Debug`].
#[derive(Clone)]
pub struct EncryptionKey([u8; KEY_LEN]);

impl EncryptionKey {
    /// Create a key from its raw bytes.
    pub fn new(key: [u8; KEY_LEN]) -> Self {
        Self(key)
    }

    /// Create a key from a slice, which must be exactly [`KEY_LEN`] bytes long.
    pub fn try_from_slice(key: &[u8]) -> VortexResult<Self> {
        let key = <[u8; KEY_LEN]>::try_from(key).map_err(|_| {
            vortex_err!(
                "Encryption key must be {KEY_LEN} bytes, but got {} bytes",
                key.len()
            )
        })?;
        Ok(Self(key))
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(&self.0.into())
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl Debug for EncryptionKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("EncryptionKey(<redacted>)")
    }
}

/// Resolves the [`EncryptionKey`] for the key metadata stored in an encrypted file.
///
/// Implementations typically look the key up in a key management service, and should cache keys
/// since they are called for every distinct [`EncryptionSpec`] each time a file is opened. They
/// must return an error if the key is unknown. A wrong key is detected when the first segment
/// fails to decrypt.
pub trait KeyRetriever: Send + Sync {
    /// Return the key identified by `key_metadata`.
    fn retrieve_key(&self, key_metadata: &[u8]) -> VortexResult<EncryptionKey>;
}

impl<F> KeyRetriever for F
where
    F: Fn(&[u8]) -> VortexResult<EncryptionKey> + Send + Sync,
{
    fn retrieve_key(&self, key_metadata: &[u8]) -> VortexResult<EncryptionKey> {
        self(key_metadata)
    }
}

/// A [`KeyRetriever`] that returns the same key for any key metadata.
impl KeyRetriever for EncryptionKey {
    fn retrieve_key(&self, _key_metadata: &[u8]) -> VortexResult<EncryptionKey> {
        Ok(self.clone())
    }
}

/// Configures how a Vortex file is encrypted when it is written.
#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    key: EncryptionKey,
    key_metadata: Arc<[u8]>,
}

impl EncryptionConfig {
    /// Encrypt every segment of the file with AES-256-GCM under `key`.
    ///
    /// `key_metadata` is stored in plaintext in the file and passed to the reader's
    /// [`KeyRetriever`] to resolve the key again, e.g. a key id in a key management service. It
    /// must not contain the key itself.
    pub fn new(key: EncryptionKey, key_metadata: impl Into<Arc<[u8]>>) -> Self {
        Self {
            key,
            key_metadata: key_metadata.into(),
        }
    }

    /// Create the cipher for one file, with fresh random per-file AAD bytes.
    pub(crate) fn file_cipher(&self, aad_prefix: Option<&[u8]>) -> VortexResult<SegmentCipher> {
        let mut aad_file_unique = [0u8; AAD_FILE_UNIQUE_LEN];
        fill_random(&mut aad_file_unique)?;
        let spec = EncryptionSpec {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_metadata: Arc::clone(&self.key_metadata),
            aad_file_unique: Arc::from(aad_file_unique),
            requires_aad_prefix: aad_prefix.is_some(),
        };
        Ok(SegmentCipher::new(
            spec,
            &self.key,
            aad_prefix.unwrap_or_default(),
        ))
    }
}

/// The encryption algorithm of a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EncryptionAlgorithm {
    /// The segment is stored in plaintext.
    None,
    /// The segment is encrypted with AES-256 in Galois/Counter Mode.
    Aes256Gcm,
}

/// Describes how a segment is encrypted, without the key itself.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EncryptionSpec {
    algorithm: EncryptionAlgorithm,
    key_metadata: Arc<[u8]>,
    aad_file_unique: Arc<[u8]>,
    requires_aad_prefix: bool,
}

impl EncryptionSpec {
    /// The spec of a plaintext segment.
    pub fn plaintext() -> Self {
        Self {
            algorithm: EncryptionAlgorithm::None,
            key_metadata: Arc::from([]),
            aad_file_unique: Arc::from([]),
            requires_aad_prefix: false,
        }
    }

    /// The encryption algorithm.
    pub fn algorithm(&self) -> EncryptionAlgorithm {
        self.algorithm
    }

    /// The opaque metadata used to resolve the key with a [`KeyRetriever`].
    pub fn key_metadata(&self) -> &[u8] {
        &self.key_metadata
    }

    /// Whether readers must supply the AAD prefix the segments were encrypted with.
    pub fn requires_aad_prefix(&self) -> bool {
        self.requires_aad_prefix
    }

    /// Whether segments with this spec are encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.algorithm != EncryptionAlgorithm::None
    }

    pub(crate) fn read_flatbuffer(fb: &fb::EncryptionSpec<'_>) -> VortexResult<Self> {
        let algorithm = match fb.algorithm() {
            fb::EncryptionAlgorithm::None => EncryptionAlgorithm::None,
            fb::EncryptionAlgorithm::Aes256Gcm => EncryptionAlgorithm::Aes256Gcm,
            other => vortex_bail!("Unsupported encryption algorithm {}", other.0),
        };
        Ok(Self {
            algorithm,
            key_metadata: fb
                .key_metadata()
                .map(|bytes| Arc::from(bytes.bytes()))
                .unwrap_or_else(|| Arc::from([])),
            aad_file_unique: fb
                .aad_file_unique()
                .map(|bytes| Arc::from(bytes.bytes()))
                .unwrap_or_else(|| Arc::from([])),
            requires_aad_prefix: fb.requires_aad_prefix(),
        })
    }

    pub(crate) fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<fb::EncryptionSpec<'fb>> {
        let algorithm = match self.algorithm {
            EncryptionAlgorithm::None => fb::EncryptionAlgorithm::None,
            EncryptionAlgorithm::Aes256Gcm => fb::EncryptionAlgorithm::Aes256Gcm,
        };
        let key_metadata =
            (!self.key_metadata.is_empty()).then(|| fbb.create_vector(&self.key_metadata));
        let aad_file_unique =
            (!self.aad_file_unique.is_empty()).then(|| fbb.create_vector(&self.aad_file_unique));
        fb::EncryptionSpec::create(
            fbb,
            &fb::EncryptionSpecArgs {
                algorithm,
                key_metadata,
                aad_file_unique,
                requires_aad_prefix: self.requires_aad_prefix,
            },
        )
    }
}

/// Identifies what an encrypted segment holds, and is authenticated alongside its ciphertext.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SegmentRole<'a> {
    /// A segment of the segment map, by index.
    Data(u32),
    DType,
    Layout,
    Statistics,
    Footer,
    /// A user-defined metadata segment, by key.
    Metadata(&'a str),
}

impl SegmentRole<'_> {
    fn write_aad(&self, aad: &mut Vec<u8>) {
        match self {
            SegmentRole::Data(index) => {
                aad.push(0);
                aad.extend_from_slice(&index.to_le_bytes());
            }
            SegmentRole::DType => aad.push(1),
            SegmentRole::Layout => aad.push(2),
            SegmentRole::Statistics => aad.push(3),
            SegmentRole::Footer => aad.push(4),
            SegmentRole::Metadata(key) => {
                aad.push(5);
                aad.extend_from_slice(key.as_bytes());
            }
        }
    }
}

/// Encrypts and decrypts the segments of one [`EncryptionSpec`].
#[derive(Clone)]
pub(crate) struct SegmentCipher {
    spec: EncryptionSpec,
    cipher: Aes256Gcm,
    /// The additional authenticated data shared by every segment, without the segment's role.
    aad: Arc<[u8]>,
}

impl SegmentCipher {
    fn new(spec: EncryptionSpec, key: &EncryptionKey, aad_prefix: &[u8]) -> Self {
        // Length-prefix the application-defined prefix, so that it cannot run into the bytes that
        // follow it.
        let prefix_len =
            u32::try_from(aad_prefix.len()).vortex_expect("AAD prefix length fits in u32");
        let mut aad = Vec::with_capacity(4 + aad_prefix.len() + spec.aad_file_unique.len());
        aad.extend_from_slice(&prefix_len.to_le_bytes());
        aad.extend_from_slice(aad_prefix);
        aad.extend_from_slice(&spec.aad_file_unique);
        Self {
            spec,
            cipher: key.cipher(),
            aad: aad.into(),
        }
    }

    /// Resolve the cipher for `spec`, or `None` if it describes plaintext segments.
    ///
    /// `aad_prefix` is the prefix supplied by the reader, which `spec` must require.
    pub(crate) fn resolve(
        spec: &EncryptionSpec,
        keys: Option<&dyn KeyRetriever>,
        aad_prefix: Option<&[u8]>,
    ) -> VortexResult<Option<Self>> {
        match spec.algorithm {
            EncryptionAlgorithm::None => Ok(None),
            EncryptionAlgorithm::Aes256Gcm => {
                let keys = keys.ok_or_else(|| {
                    vortex_err!(
                        "Vortex file is encrypted, but no KeyRetriever was provided to \
                         VortexOpenOptions::with_key_retriever"
                    )
                })?;
                let aad_prefix = match (aad_prefix, spec.requires_aad_prefix) {
                    (Some(supplied), true) => supplied,
                    (None, false) => &[],
                    (Some(_), false) => vortex_bail!(
                        "An AAD prefix was supplied with VortexOpenOptions::with_aad_prefix, but \
                         the Vortex file is not bound to one"
                    ),
                    (None, true) => vortex_bail!(
                        "Vortex file requires an AAD prefix, supplied with \
                         VortexOpenOptions::with_aad_prefix"
                    ),
                };
                let key = keys.retrieve_key(&spec.key_metadata)?;
                Ok(Some(Self::new(spec.clone(), &key, aad_prefix)))
            }
        }
    }

    pub(crate) fn spec(&self) -> &EncryptionSpec {
        &self.spec
    }

    fn aad(&self, role: SegmentRole<'_>) -> Vec<u8> {
        let mut aad = Vec::with_capacity(self.aad.len() + 8);
        aad.extend_from_slice(&self.aad);
        role.write_aad(&mut aad);
        aad
    }

    /// Encrypt the concatenation of `buffers` into a single buffer aligned to `alignment`.
    pub(crate) fn encrypt(
        &self,
        role: SegmentRole<'_>,
        buffers: &[ByteBuffer],
        alignment: Alignment,
    ) -> VortexResult<ByteBuffer> {
        let len = buffers.iter().map(ByteBuffer::len).sum::<usize>();
        let mut out = ByteBufferMut::with_capacity_aligned(len + ENCRYPTION_OVERHEAD, alignment);
        for buffer in buffers {
            out.extend_from_slice(buffer.as_slice());
        }

        let mut nonce = [0u8; NONCE_LEN];
        fill_random(&mut nonce)?;
        let tag = self
            .cipher
            .encrypt_inout_detached(
                &Nonce::from(nonce),
                &self.aad(role),
                out.as_mut_slice().into(),
            )
            .map_err(|_| vortex_err!("Failed to encrypt {role:?} segment"))?;

        out.extend_from_slice(&nonce);
        out.extend_from_slice(tag.as_slice());
        Ok(out.freeze())
    }

    /// Decrypt and authenticate an encrypted segment, returning a plaintext aligned to
    /// `alignment`.
    ///
    /// The ciphertext is decrypted in place when `encrypted` is not shared.
    pub(crate) fn decrypt(
        &self,
        role: SegmentRole<'_>,
        encrypted: ByteBuffer,
        alignment: Alignment,
    ) -> VortexResult<ByteBuffer> {
        let Some(len) = encrypted.len().checked_sub(ENCRYPTION_OVERHEAD) else {
            vortex_bail!(
                "Encrypted {role:?} segment of {} bytes is shorter than the {ENCRYPTION_OVERHEAD} \
                 bytes of encryption overhead",
                encrypted.len()
            );
        };
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&encrypted[len..len + NONCE_LEN]);
        let mut tag = [0u8; TAG_LEN];
        tag.copy_from_slice(&encrypted[len + NONCE_LEN..]);

        let mut plaintext = match encrypted.try_into_mut() {
            Ok(buffer) if buffer.alignment().is_aligned_to(alignment) => buffer,
            Ok(buffer) => ByteBufferMut::copy_from_aligned(buffer, alignment),
            Err(buffer) => ByteBufferMut::copy_from_aligned(buffer, alignment),
        };
        plaintext.truncate(len);
        self.cipher
            .decrypt_inout_detached(
                &Nonce::from(nonce),
                &self.aad(role),
                plaintext.as_mut_slice().into(),
                &Tag::from(tag),
            )
            .map_err(|_| {
                vortex_err!(
                    "Failed to decrypt {role:?} segment: the key or AAD prefix is wrong, or the \
                     segment was modified"
                )
            })?;
        Ok(plaintext.freeze())
    }
}

fn fill_random(bytes: &mut [u8]) -> VortexResult<()> {
    getrandom::fill(bytes).map_err(|e| vortex_err!("Failed to generate random bytes: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher() -> VortexResult<SegmentCipher> {
        EncryptionConfig::new(EncryptionKey::new([7; KEY_LEN]), b"key-1".as_slice())
            .file_cipher(None)
    }

    #[test]
    fn roundtrip() -> VortexResult<()> {
        let cipher = cipher()?;
        let buffers = [
            ByteBuffer::copy_from(b"hello "),
            ByteBuffer::copy_from(b"world"),
        ];
        let alignment = Alignment::new(64);

        let encrypted = cipher.encrypt(SegmentRole::Data(3), &buffers, alignment)?;
        assert_eq!(encrypted.len(), 11 + ENCRYPTION_OVERHEAD);
        assert_ne!(&encrypted[..11], b"hello world");

        let decrypted = cipher.decrypt(SegmentRole::Data(3), encrypted, alignment)?;
        assert_eq!(decrypted.as_slice(), b"hello world");
        assert!(decrypted.alignment().is_aligned_to(alignment));
        Ok(())
    }

    #[test]
    fn rejects_other_role() -> VortexResult<()> {
        let cipher = cipher()?;
        let encrypted = cipher.encrypt(
            SegmentRole::Data(3),
            &[ByteBuffer::copy_from(b"data")],
            Alignment::none(),
        )?;
        let err = cipher
            .decrypt(SegmentRole::Data(4), encrypted, Alignment::none())
            .unwrap_err();
        assert!(err.to_string().contains("Failed to decrypt"), "{err}");
        Ok(())
    }

    #[test]
    fn rejects_other_file() -> VortexResult<()> {
        // Same key and key metadata, but a different file, and so a different AAD prefix.
        let encrypted = cipher()?.encrypt(
            SegmentRole::Footer,
            &[ByteBuffer::copy_from(b"footer")],
            Alignment::none(),
        )?;
        assert!(
            cipher()?
                .decrypt(SegmentRole::Footer, encrypted, Alignment::none())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rejects_wrong_key() -> VortexResult<()> {
        let cipher = cipher()?;
        let encrypted = cipher.encrypt(
            SegmentRole::Layout,
            &[ByteBuffer::copy_from(b"layout")],
            Alignment::none(),
        )?;
        let wrong = SegmentCipher::new(
            cipher.spec().clone(),
            &EncryptionKey::new([8; KEY_LEN]),
            b"",
        );
        assert!(
            wrong
                .decrypt(SegmentRole::Layout, encrypted, Alignment::none())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rejects_truncated_segment() -> VortexResult<()> {
        let err = cipher()?
            .decrypt(
                SegmentRole::DType,
                ByteBuffer::copy_from([0u8; ENCRYPTION_OVERHEAD - 1]),
                Alignment::none(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("shorter"), "{err}");
        Ok(())
    }

    #[test]
    fn spec_flatbuffer_roundtrip() -> VortexResult<()> {
        let spec = cipher()?.spec().clone();
        let mut fbb = FlatBufferBuilder::new();
        let offset = spec.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(offset);
        let read = EncryptionSpec::read_flatbuffer(&flatbuffers::root::<fb::EncryptionSpec>(
            fbb.finished_data(),
        )?)?;
        assert_eq!(read, spec);
        Ok(())
    }
}
