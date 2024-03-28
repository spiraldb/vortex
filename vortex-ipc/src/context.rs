use crate::flatbuffers::ipc as fb;
use crate::missing;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex::encoding::{find_encoding, EncodingId, EncodingRef, ENCODINGS};
use vortex_error::VortexError;
use vortex_flatbuffers::WriteFlatBuffer;

#[derive(Debug)]
pub struct IPCContext {
    encodings: Vec<EncodingRef>,
}

impl IPCContext {
    pub fn find_encoding(&self, encoding_idx: u16) -> Option<EncodingRef> {
        self.encodings.get(encoding_idx as usize).cloned()
    }

    pub fn encoding_position(&self, encoding_id: EncodingId) -> Option<u16> {
        self.encodings
            .iter()
            .position(|e| e.id() == encoding_id)
            .map(|i| i as u16)
    }
}

impl Default for IPCContext {
    fn default() -> Self {
        Self {
            encodings: ENCODINGS.iter().cloned().collect_vec(),
        }
    }
}

impl<'a> TryFrom<fb::Context<'a>> for IPCContext {
    type Error = VortexError;

    fn try_from(value: fb::Context<'a>) -> Result<Self, Self::Error> {
        let fb_encodings = value.encodings().ok_or_else(missing("encodings"))?;
        let mut encodings = Vec::with_capacity(fb_encodings.len());
        for fb_encoding in fb_encodings {
            let encoding_id = fb_encoding.id().ok_or_else(missing("encoding.id"))?;
            encodings.push(find_encoding(encoding_id).ok_or_else(|| {
                VortexError::InvalidArgument(
                    format!("Stream uses unknown encoding {}", encoding_id).into(),
                )
            })?);
        }
        Ok(Self { encodings })
    }
}

impl WriteFlatBuffer for IPCContext {
    type Target<'a> = fb::Context<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let fb_encodings = self
            .encodings
            .iter()
            .map(|e| e.id().name())
            .map(|name| {
                let encoding_id = fbb.create_string(name);
                fb::Encoding::create(
                    fbb,
                    &fb::EncodingArgs {
                        id: Some(encoding_id),
                    },
                )
            })
            .collect_vec();
        let fb_encodings = fbb.create_vector(fb_encodings.as_slice());

        fb::Context::create(
            fbb,
            &fb::ContextArgs {
                encodings: Some(fb_encodings),
            },
        )
    }
}
