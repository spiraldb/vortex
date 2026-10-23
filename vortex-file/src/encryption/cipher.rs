// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Encryption, decryption and signing of individual segments.

use std::sync::Arc;

use aes_gcm::AeadInOut;
use aes_gcm::Aes128Gcm;
use aes_gcm::Aes256Gcm;
use aes_gcm::AesGcm;
use aes_gcm::KeyInit;
use aes_gcm::Nonce;
use aes_gcm::Tag;
use aes_gcm::aead::consts::U12;
use aes_gcm::aes::Aes128;
use aes_gcm::aes::Aes192;
use aes_gcm::aes::Aes256;
use ctr::Ctr32BE;
use ctr::cipher::KeyIvInit;
use ctr::cipher::StreamCipher;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;

use super::EncryptionAlgorithm;
use super::EncryptionKey;
use super::EncryptionSpec;
use super::FileDecryption;
use super::fill_random;

pub(crate) const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// Length in bytes of the signature of a plaintext segment, `nonce || tag`.
pub(crate) const SIGNATURE_LEN: usize = NONCE_LEN + TAG_LEN;

type Aes192Gcm = AesGcm<Aes192, U12>;

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

/// Distinguishes the AAD of a signature from that of an encrypted segment with the same role.
const SIGNATURE_AAD_MARKER: u8 = 0xFF;

/// AES-GCM with any of the supported key lengths.
#[derive(Clone)]
pub(crate) enum Aead {
    Aes128(Aes128Gcm),
    Aes192(Aes192Gcm),
    Aes256(Aes256Gcm),
}

macro_rules! dispatch {
    ($aead:expr, $cipher:ident => $body:expr) => {
        match $aead {
            Aead::Aes128($cipher) => $body,
            Aead::Aes192($cipher) => $body,
            Aead::Aes256($cipher) => $body,
        }
    };
}

impl Aead {
    pub(crate) fn new(key: &EncryptionKey) -> VortexResult<Self> {
        let bytes = key.as_bytes();
        let invalid = |_| vortex_err!("Invalid AES key length {}", bytes.len());
        Ok(match bytes.len() {
            16 => Self::Aes128(Aes128Gcm::new_from_slice(bytes).map_err(invalid)?),
            24 => Self::Aes192(Aes192Gcm::new_from_slice(bytes).map_err(invalid)?),
            32 => Self::Aes256(Aes256Gcm::new_from_slice(bytes).map_err(invalid)?),
            len => vortex_bail!("Invalid AES key length {len}"),
        })
    }

    /// Encrypt `buffer` in place, returning `nonce || tag`.
    pub(crate) fn seal(&self, aad: &[u8], buffer: &mut [u8]) -> VortexResult<[u8; SIGNATURE_LEN]> {
        let mut nonce = [0u8; NONCE_LEN];
        fill_random(&mut nonce)?;
        let tag = dispatch!(self, cipher => cipher.encrypt_inout_detached(&Nonce::from(nonce), aad, buffer.into()))
            .map_err(|_| vortex_err!("AES-GCM encryption failed"))?;
        let mut out = [0u8; SIGNATURE_LEN];
        out[..NONCE_LEN].copy_from_slice(&nonce);
        out[NONCE_LEN..].copy_from_slice(tag.as_slice());
        Ok(out)
    }

    /// Decrypt `buffer` in place, authenticating it against `nonce || tag`.
    pub(crate) fn open(&self, aad: &[u8], buffer: &mut [u8], nonce_tag: &[u8]) -> Result<(), ()> {
        if nonce_tag.len() != SIGNATURE_LEN {
            return Err(());
        }
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&nonce_tag[..NONCE_LEN]);
        let mut tag = [0u8; TAG_LEN];
        tag.copy_from_slice(&nonce_tag[NONCE_LEN..]);
        let (nonce, tag) = (Nonce::from(nonce), Tag::from(tag));
        dispatch!(self, cipher => cipher.decrypt_inout_detached(&nonce, aad, buffer.into(), &tag))
            .map_err(|_| ())
    }
}

/// Apply the AES-CTR keystream for `nonce` to `buffer`. As in Parquet, the 16-byte IV is the
/// nonce followed by a 32-bit big-endian block counter starting at 1.
fn apply_ctr(key: &EncryptionKey, nonce: &[u8; NONCE_LEN], buffer: &mut [u8]) -> VortexResult<()> {
    let mut iv = [0u8; 16];
    iv[..NONCE_LEN].copy_from_slice(nonce);
    iv[15] = 1;
    let bytes = key.as_bytes();
    let invalid = |_| vortex_err!("Invalid AES key length {}", bytes.len());
    match bytes.len() {
        16 => Ctr32BE::<Aes128>::new_from_slices(bytes, &iv)
            .map_err(invalid)?
            .try_apply_keystream(buffer),
        24 => Ctr32BE::<Aes192>::new_from_slices(bytes, &iv)
            .map_err(invalid)?
            .try_apply_keystream(buffer),
        32 => Ctr32BE::<Aes256>::new_from_slices(bytes, &iv)
            .map_err(invalid)?
            .try_apply_keystream(buffer),
        len => vortex_bail!("Invalid AES key length {len}"),
    }
    .map_err(|_| vortex_err!("Segment of {} bytes is too long for AES-CTR", buffer.len()))
}

/// Encrypts, decrypts and signs the segments of one [`EncryptionSpec`].
#[derive(Clone)]
pub(crate) struct SegmentCipher {
    spec: EncryptionSpec,
    key: EncryptionKey,
    aead: Aead,
    /// The additional authenticated data shared by every segment, without the segment's role.
    aad: Arc<[u8]>,
}

impl SegmentCipher {
    pub(crate) fn new(
        spec: EncryptionSpec,
        key: EncryptionKey,
        aad_prefix: &[u8],
    ) -> VortexResult<Self> {
        // Length-prefix the application-defined prefix, so that it cannot run into the bytes that
        // follow it.
        let prefix_len =
            u32::try_from(aad_prefix.len()).vortex_expect("AAD prefix length fits in u32");
        let mut aad = Vec::with_capacity(4 + aad_prefix.len() + spec.aad_file_unique.len());
        aad.extend_from_slice(&prefix_len.to_le_bytes());
        aad.extend_from_slice(aad_prefix);
        aad.extend_from_slice(&spec.aad_file_unique);
        Ok(Self {
            spec,
            aead: Aead::new(&key)?,
            key,
            aad: aad.into(),
        })
    }

    /// Resolve the cipher for `spec`, or `None` if it describes plaintext segments.
    pub(crate) fn resolve(
        spec: &EncryptionSpec,
        decryption: Option<&FileDecryption>,
    ) -> VortexResult<Option<Self>> {
        if !spec.is_encrypted() {
            return Ok(None);
        }
        let decryption = decryption.ok_or_else(|| {
            vortex_err!(
                "Vortex file is encrypted, but no FileDecryption was provided to \
                 VortexOpenOptions::with_decryption"
            )
        })?;
        let aad_prefix = match (decryption.aad_prefix.as_deref(), spec.requires_aad_prefix) {
            (Some(supplied), true) => supplied,
            (None, false) => &[],
            (Some(_), false) => vortex_bail!(
                "An AAD prefix was supplied with FileDecryption::with_aad_prefix, but the Vortex \
                 file is not bound to one"
            ),
            (None, true) => vortex_bail!(
                "Vortex file requires an AAD prefix, supplied with FileDecryption::with_aad_prefix"
            ),
        };
        let key = decryption.keys.retrieve_key(&spec.key_metadata)?;
        Ok(Some(Self::new(spec.clone(), key, aad_prefix)?))
    }

    pub(crate) fn spec(&self) -> &EncryptionSpec {
        &self.spec
    }

    /// Whether segments with `role` are encrypted with AES-CTR rather than AES-GCM.
    fn uses_ctr(&self, role: SegmentRole<'_>) -> bool {
        self.spec.algorithm == EncryptionAlgorithm::AesGcmCtr
            && matches!(role, SegmentRole::Data(_))
    }

    /// The number of bytes an encrypted segment with `role` occupies beyond its plaintext.
    pub(crate) fn overhead(&self, role: SegmentRole<'_>) -> usize {
        if self.uses_ctr(role) {
            NONCE_LEN
        } else {
            SIGNATURE_LEN
        }
    }

    fn aad(&self, role: SegmentRole<'_>) -> Vec<u8> {
        let mut aad = Vec::with_capacity(self.aad.len() + 8);
        aad.extend_from_slice(&self.aad);
        role.write_aad(&mut aad);
        aad
    }

    /// Encrypt the concatenation of `buffers` into a single buffer aligned to `alignment`.
    ///
    /// The result is `ciphertext || nonce || tag` for AES-GCM, and `ciphertext || nonce` for
    /// AES-CTR, with a ciphertext of the plaintext's length.
    pub(crate) fn encrypt(
        &self,
        role: SegmentRole<'_>,
        buffers: &[ByteBuffer],
        alignment: Alignment,
    ) -> VortexResult<ByteBuffer> {
        let len = buffers.iter().map(ByteBuffer::len).sum::<usize>();
        let mut out = ByteBufferMut::with_capacity_aligned(len + self.overhead(role), alignment);
        for buffer in buffers {
            out.extend_from_slice(buffer.as_slice());
        }

        if self.uses_ctr(role) {
            let mut nonce = [0u8; NONCE_LEN];
            fill_random(&mut nonce)?;
            apply_ctr(&self.key, &nonce, out.as_mut_slice())?;
            out.extend_from_slice(&nonce);
        } else {
            let nonce_tag = self
                .aead
                .seal(&self.aad(role), out.as_mut_slice())
                .map_err(|e| vortex_err!("Failed to encrypt {role:?} segment: {e}"))?;
            out.extend_from_slice(&nonce_tag);
        }
        Ok(out.freeze())
    }

    /// Decrypt an encrypted segment, returning a plaintext aligned to `alignment`.
    ///
    /// AES-GCM segments are authenticated. The ciphertext is decrypted in place when `encrypted`
    /// is not shared.
    pub(crate) fn decrypt(
        &self,
        role: SegmentRole<'_>,
        encrypted: ByteBuffer,
        alignment: Alignment,
    ) -> VortexResult<ByteBuffer> {
        let overhead = self.overhead(role);
        let Some(len) = encrypted.len().checked_sub(overhead) else {
            vortex_bail!(
                "Encrypted {role:?} segment of {} bytes is shorter than the {overhead} bytes of \
                 encryption overhead",
                encrypted.len()
            );
        };
        let mut trailer = [0u8; SIGNATURE_LEN];
        trailer[..overhead].copy_from_slice(&encrypted[len..]);

        let mut plaintext = match encrypted.try_into_mut() {
            Ok(buffer) if buffer.alignment().is_aligned_to(alignment) => buffer,
            Ok(buffer) => ByteBufferMut::copy_from_aligned(buffer, alignment),
            Err(buffer) => ByteBufferMut::copy_from_aligned(buffer, alignment),
        };
        plaintext.truncate(len);

        if self.uses_ctr(role) {
            let mut nonce = [0u8; NONCE_LEN];
            nonce.copy_from_slice(&trailer[..NONCE_LEN]);
            apply_ctr(&self.key, &nonce, plaintext.as_mut_slice())?;
        } else {
            self.aead
                .open(&self.aad(role), plaintext.as_mut_slice(), &trailer)
                .map_err(|()| {
                    vortex_err!(
                        "Failed to decrypt {role:?} segment: the key or AAD prefix is wrong, or \
                         the segment was modified"
                    )
                })?;
        }
        Ok(plaintext.freeze())
    }

    fn signature_aad(&self, role: SegmentRole<'_>, plaintext: &[u8]) -> Vec<u8> {
        let mut aad = self.aad(role);
        aad.push(SIGNATURE_AAD_MARKER);
        aad.extend_from_slice(plaintext);
        aad
    }

    /// Sign a plaintext segment with AES-GMAC, returning `nonce || tag`.
    pub(crate) fn sign(
        &self,
        role: SegmentRole<'_>,
        plaintext: &[u8],
    ) -> VortexResult<[u8; SIGNATURE_LEN]> {
        self.aead
            .seal(&self.signature_aad(role, plaintext), &mut [])
            .map_err(|e| vortex_err!("Failed to sign {role:?} segment: {e}"))
    }

    /// Verify the signature of a plaintext segment.
    pub(crate) fn verify(
        &self,
        role: SegmentRole<'_>,
        plaintext: &[u8],
        signature: &[u8],
    ) -> VortexResult<()> {
        self.aead
            .open(&self.signature_aad(role, plaintext), &mut [], signature)
            .map_err(|()| {
                vortex_err!(
                    "Failed to verify the signature of the plaintext {role:?} segment: the footer \
                     key or AAD prefix is wrong, or the segment was modified"
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::encryption::KeyConfig;

    fn cipher(algorithm: EncryptionAlgorithm, key_len: usize) -> VortexResult<SegmentCipher> {
        KeyConfig::new(
            EncryptionKey::try_from_slice(&vec![7; key_len])?,
            b"key-1".as_slice(),
        )
        .file_cipher(algorithm, None)
    }

    #[rstest]
    #[case::gcm_128(EncryptionAlgorithm::AesGcm, 16)]
    #[case::gcm_192(EncryptionAlgorithm::AesGcm, 24)]
    #[case::gcm_256(EncryptionAlgorithm::AesGcm, 32)]
    #[case::ctr_128(EncryptionAlgorithm::AesGcmCtr, 16)]
    #[case::ctr_192(EncryptionAlgorithm::AesGcmCtr, 24)]
    #[case::ctr_256(EncryptionAlgorithm::AesGcmCtr, 32)]
    fn roundtrip(
        #[case] algorithm: EncryptionAlgorithm,
        #[case] key_len: usize,
    ) -> VortexResult<()> {
        let cipher = cipher(algorithm, key_len)?;
        let buffers = [
            ByteBuffer::copy_from(b"hello "),
            ByteBuffer::copy_from(b"world"),
        ];
        let alignment = Alignment::new(64);

        for role in [SegmentRole::Data(3), SegmentRole::Footer] {
            let encrypted = cipher.encrypt(role, &buffers, alignment)?;
            assert_eq!(encrypted.len(), 11 + cipher.overhead(role));
            assert_ne!(&encrypted[..11], b"hello world");

            let decrypted = cipher.decrypt(role, encrypted, alignment)?;
            assert_eq!(decrypted.as_slice(), b"hello world");
            assert!(decrypted.alignment().is_aligned_to(alignment));
        }
        Ok(())
    }

    #[test]
    fn ctr_applies_to_data_segments_only() -> VortexResult<()> {
        let cipher = cipher(EncryptionAlgorithm::AesGcmCtr, 32)?;
        assert_eq!(cipher.overhead(SegmentRole::Data(0)), NONCE_LEN);
        assert_eq!(cipher.overhead(SegmentRole::Footer), SIGNATURE_LEN);
        assert_eq!(cipher.overhead(SegmentRole::Metadata("key")), SIGNATURE_LEN);
        Ok(())
    }

    /// AES-CTR provides no integrity: a modified data segment decrypts to modified plaintext.
    #[test]
    fn ctr_is_not_authenticated() -> VortexResult<()> {
        let cipher = cipher(EncryptionAlgorithm::AesGcmCtr, 32)?;
        let encrypted = cipher.encrypt(
            SegmentRole::Data(0),
            &[ByteBuffer::copy_from(b"data")],
            Alignment::none(),
        )?;
        let mut tampered = ByteBufferMut::copy_from(&encrypted);
        tampered[0] ^= 1;
        let decrypted =
            cipher.decrypt(SegmentRole::Data(0), tampered.freeze(), Alignment::none())?;
        assert_eq!(decrypted.as_slice(), b"eata");
        Ok(())
    }

    #[test]
    fn rejects_other_role() -> VortexResult<()> {
        let cipher = cipher(EncryptionAlgorithm::AesGcm, 32)?;
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
        // Same key and key metadata, but a different file, and so different per-file AAD bytes.
        let encrypted = cipher(EncryptionAlgorithm::AesGcm, 32)?.encrypt(
            SegmentRole::Footer,
            &[ByteBuffer::copy_from(b"footer")],
            Alignment::none(),
        )?;
        assert!(
            cipher(EncryptionAlgorithm::AesGcm, 32)?
                .decrypt(SegmentRole::Footer, encrypted, Alignment::none())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rejects_wrong_key() -> VortexResult<()> {
        let cipher = cipher(EncryptionAlgorithm::AesGcm, 32)?;
        let encrypted = cipher.encrypt(
            SegmentRole::Layout,
            &[ByteBuffer::copy_from(b"layout")],
            Alignment::none(),
        )?;
        let wrong = SegmentCipher::new(cipher.spec().clone(), EncryptionKey::from([8; 32]), b"")?;
        assert!(
            wrong
                .decrypt(SegmentRole::Layout, encrypted, Alignment::none())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rejects_truncated_segment() -> VortexResult<()> {
        let err = cipher(EncryptionAlgorithm::AesGcm, 32)?
            .decrypt(
                SegmentRole::DType,
                ByteBuffer::copy_from([0u8; SIGNATURE_LEN - 1]),
                Alignment::none(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("shorter"), "{err}");
        Ok(())
    }

    #[test]
    fn signature() -> VortexResult<()> {
        let signer = cipher(EncryptionAlgorithm::AesGcm, 32)?;
        let signature = signer.sign(SegmentRole::Footer, b"footer")?;
        signer.verify(SegmentRole::Footer, b"footer", &signature)?;

        assert!(
            signer
                .verify(SegmentRole::Footer, b"modified", &signature)
                .is_err()
        );
        assert!(
            signer
                .verify(SegmentRole::Layout, b"footer", &signature)
                .is_err()
        );
        assert!(
            cipher(EncryptionAlgorithm::AesGcm, 32)?
                .verify(SegmentRole::Footer, b"footer", &signature)
                .is_err()
        );
        Ok(())
    }
}
