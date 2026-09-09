// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use memmap2::Mmap;
use memmap2::MmapMut;

use crate::ByteBuffer;
use crate::ByteBufferMut;

impl From<Mmap> for ByteBuffer {
    fn from(value: Mmap) -> Self {
        ByteBuffer::from_owner(value)
    }
}

impl From<MmapMut> for ByteBufferMut {
    fn from(value: MmapMut) -> Self {
        // A writable mapping is exclusively ours, so the buffer can write straight into it.
        ByteBufferMut::from_owner(value)
    }
}

#[cfg(test)]
mod tests {
    use memmap2::MmapMut;
    use memmap2::MmapOptions;

    use crate::ByteBuffer;
    use crate::ByteBufferMut;

    #[test]
    fn anonymous_mapping_is_adopted_writable() {
        let mut mapping = MmapOptions::new()
            .len(4096)
            .map_anon()
            .expect("anonymous mapping");
        mapping[..4].copy_from_slice(&[1, 2, 3, 4]);
        let ptr = mapping.as_ptr();

        let mut buffer = ByteBufferMut::from(mapping);
        assert_eq!(buffer.as_ptr(), ptr, "adoption is zero-copy");
        assert_eq!(buffer.len(), 4096);

        // The mapping is exclusively ours, so writes land in it rather than in a copy.
        buffer[0] = 10;
        assert_eq!(&buffer[..4], &[10, 2, 3, 4]);
        assert_eq!(buffer.as_ptr(), ptr);

        // And it survives a freeze/thaw cycle without moving.
        let frozen = buffer.freeze();
        assert_eq!(frozen.as_ptr(), ptr);
        let thawed = frozen.try_into_mut().expect("sole handle to the mapping");
        assert_eq!(thawed.as_ptr(), ptr);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri does not support mprotect")]
    fn read_only_mapping_is_adopted_read_only() {
        let mapping = MmapOptions::new()
            .len(4096)
            .map_anon()
            .expect("anonymous mapping")
            .make_read_only()
            .expect("read-only mapping");
        let ptr = mapping.as_ptr();

        let buffer = ByteBuffer::from(mapping);
        assert_eq!(buffer.as_ptr(), ptr, "adoption is zero-copy");
        // Writing through a read-only mapping would fault, so this must copy instead.
        assert!(buffer.try_into_mut().is_err());
    }

    #[test]
    fn shared_mapping_cannot_be_reclaimed() {
        let mapping: MmapMut = MmapOptions::new().len(64).map_anon().expect("mapping");
        let frozen = ByteBufferMut::from(mapping).freeze();
        let _shared = frozen.clone();
        assert!(frozen.try_into_mut().is_err());
    }
}
