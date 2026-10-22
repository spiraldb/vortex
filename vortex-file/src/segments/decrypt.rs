// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use futures::FutureExt;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
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
    /// The cipher of each footer encryption spec, or `None` for plaintext.
    ciphers: Arc<[Option<Arc<SegmentCipher>>]>,
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
    ) -> VortexResult<Arc<dyn SegmentSource>> {
        if !footer.is_encrypted() {
            return Ok(inner);
        }
        let ciphers = footer
            .encryption_specs()
            .iter()
            .map(|spec| Ok(SegmentCipher::resolve(spec, keys)?.map(Arc::new)))
            .collect::<VortexResult<_>>()?;
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

impl SegmentSource for DecryptingSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let index = *id as usize;
        let Some((spec, Some(cipher))) = self.segments.get(index).map(|spec| {
            let cipher = self
                .ciphers
                .get(usize::from(spec.encryption))
                .cloned()
                .flatten();
            (spec, cipher)
        }) else {
            // Plaintext segments, and unknown ids for the inner source to reject.
            return self.inner.request(id);
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
