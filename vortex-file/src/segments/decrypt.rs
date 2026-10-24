// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use futures::FutureExt;
use futures::future;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;

use crate::Footer;
use crate::SegmentSpec;
use crate::encryption::FileDecryption;
use crate::encryption::SegmentCipher;
use crate::encryption::SegmentRole;

/// A [`SegmentSource`] that decrypts the encrypted segments of a file.
///
/// It sits above any segment cache, so that caches only ever hold ciphertext.
pub(crate) struct DecryptingSegmentSource {
    inner: Arc<dyn SegmentSource>,
    /// The segment map followed by the metadata segments, indexed by segment id.
    segments: Arc<[SegmentSpec]>,
    /// The cipher of each footer encryption spec.
    ciphers: Arc<[CipherSlot]>,
    /// The number of segments in the segment map. Later ids are metadata segments.
    num_data_segments: usize,
    /// The keys of the metadata segments, in segment id order.
    metadata_keys: Arc<[String]>,
}

impl DecryptingSegmentSource {
    /// Wrap `inner` to decrypt the segments of `footer`, or return it unchanged if the file is not
    /// encrypted.
    pub(crate) fn wrap(
        inner: Arc<dyn SegmentSource>,
        footer: &Footer,
        decryption: Option<&FileDecryption>,
    ) -> VortexResult<Arc<dyn SegmentSource>> {
        Ok(match Self::new(Arc::clone(&inner), footer, decryption)? {
            Some(decrypting) => Arc::new(decrypting),
            None => inner,
        })
    }

    /// Create a source decrypting the segments of `footer` read from `inner`, or `None` if the
    /// file is not encrypted.
    pub(crate) fn new(
        inner: Arc<dyn SegmentSource>,
        footer: &Footer,
        decryption: Option<&FileDecryption>,
    ) -> VortexResult<Option<Self>> {
        // Writers only record encryption specs for files written with encryption.
        if footer.encryption_specs().is_empty() {
            if decryption.is_some_and(|decryption| !decryption.allow_plaintext_files) {
                vortex_bail!(
                    "Vortex file is not encrypted, but decryption was configured; allow plaintext \
                     files with FileDecryption::with_plaintext_files_allowed"
                );
            }
            return Ok(None);
        }
        // A key that cannot be retrieved only fails reads of the segments encrypted with it, so
        // that a reader without the key of an encrypted field, or without any keys for a file
        // with a plaintext footer, can still read the other fields.
        let ciphers = footer
            .encryption_specs()
            .iter()
            .map(|spec| match SegmentCipher::resolve(spec, decryption) {
                Ok(None) => CipherSlot::Plaintext,
                Ok(Some(cipher)) => CipherSlot::Ready(Arc::new(cipher)),
                Err(err) => CipherSlot::Unavailable(Arc::from(err.to_string())),
            })
            .collect();
        Ok(Some(Self {
            inner,
            segments: footer.segment_specs_with_metadata(),
            ciphers,
            num_data_segments: footer.segment_map().len(),
            metadata_keys: footer
                .metadata_segments()
                .map(|(key, _)| key.to_string())
                .collect(),
        }))
    }

    /// The cipher of segment `id`, or `None` for a plaintext or unknown segment.
    fn cipher(&self, id: SegmentId) -> VortexResult<Option<(Arc<SegmentCipher>, &SegmentSpec)>> {
        let Some(spec) = self.segments.get(*id as usize) else {
            return Ok(None);
        };
        match self.ciphers.get(usize::from(spec.encryption)) {
            Some(CipherSlot::Ready(cipher)) => Ok(Some((Arc::clone(cipher), spec))),
            Some(CipherSlot::Unavailable(err)) => {
                Err(vortex_err!("Cannot decrypt segment {}: {err}", *id))
            }
            Some(CipherSlot::Plaintext) | None => Ok(None),
        }
    }

    /// The role of segment `id` in the file's additional authenticated data.
    fn role(&self, id: SegmentId) -> SegmentRole<'_> {
        match (*id as usize)
            .checked_sub(self.num_data_segments)
            .and_then(|metadata| self.metadata_keys.get(metadata))
        {
            Some(key) => SegmentRole::Metadata(key),
            None => SegmentRole::Data(*id),
        }
    }

    /// Decrypt a segment covered by the footer's initial read, for a cache that holds plaintext.
    ///
    /// Returns `None` if the segment's key is unavailable, so that it is left out of the cache
    /// and the error surfaces when the segment is requested.
    pub(crate) fn decrypt_initial(
        &self,
        id: SegmentId,
        buffer: ByteBuffer,
    ) -> VortexResult<Option<ByteBuffer>> {
        match self.cipher(id) {
            Ok(Some((cipher, spec))) => cipher
                .decrypt(self.role(id), buffer, spec.alignment)
                .map(Some),
            Ok(None) => Ok(Some(buffer)),
            Err(_) => Ok(None),
        }
    }
}

/// The cipher of a footer encryption spec.
enum CipherSlot {
    Plaintext,
    Ready(Arc<SegmentCipher>),
    /// The key could not be retrieved, for the given reason.
    Unavailable(Arc<str>),
}

impl SegmentSource for DecryptingSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let (cipher, alignment) = match self.cipher(id) {
            Ok(Some((cipher, spec))) => (cipher, spec.alignment),
            // Plaintext segments, and unknown ids for the inner source to reject.
            Ok(None) => return self.inner.request(id),
            Err(err) => return future::ready(Err(err)).boxed(),
        };
        let metadata_key = match self.role(id) {
            SegmentRole::Metadata(key) => Some(key.to_string()),
            _ => None,
        };
        let fut = self.inner.request(id);
        async move {
            let encrypted = fut.await?.try_into_host()?.await?;
            let role = match &metadata_key {
                Some(key) => SegmentRole::Metadata(key),
                None => SegmentRole::Data(*id),
            };
            Ok(BufferHandle::new_host(
                cipher.decrypt(role, encrypted, alignment)?,
            ))
        }
        .boxed()
    }
}
