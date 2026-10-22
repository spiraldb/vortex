// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use futures::FutureExt;
use futures::future;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;

use crate::Footer;
use crate::SegmentSpec;
use crate::encryption::KeyRetriever;
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
        keys: Option<&dyn KeyRetriever>,
        aad_prefix: Option<&[u8]>,
    ) -> VortexResult<Arc<dyn SegmentSource>> {
        if !footer.is_encrypted() {
            return Ok(inner);
        }
        if keys.is_none() {
            vortex_bail!(
                "Vortex file is encrypted, but no KeyRetriever was provided to \
                 VortexOpenOptions::with_key_retriever"
            );
        }
        // A key that cannot be retrieved only fails reads of the segments encrypted with it, so
        // that a reader without the key of an encrypted field can still read the other fields.
        let ciphers = footer
            .encryption_specs()
            .iter()
            .map(
                |spec| match SegmentCipher::resolve(spec, keys, aad_prefix) {
                    Ok(None) => CipherSlot::Plaintext,
                    Ok(Some(cipher)) => CipherSlot::Ready(Arc::new(cipher)),
                    Err(err) => CipherSlot::Unavailable(Arc::from(err.to_string())),
                },
            )
            .collect();
        Ok(Arc::new(Self {
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
        let index = *id as usize;
        let Some(spec) = self.segments.get(index) else {
            // Let the inner source reject unknown ids.
            return self.inner.request(id);
        };
        let cipher = match self.ciphers.get(usize::from(spec.encryption)) {
            Some(CipherSlot::Ready(cipher)) => Arc::clone(cipher),
            Some(CipherSlot::Unavailable(err)) => {
                return future::ready(Err(vortex_err!("Cannot decrypt segment {}: {err}", *id)))
                    .boxed();
            }
            Some(CipherSlot::Plaintext) | None => return self.inner.request(id),
        };

        let alignment = spec.alignment;
        let metadata_key = index
            .checked_sub(self.num_data_segments)
            .and_then(|metadata| self.metadata_keys.get(metadata).cloned());
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
