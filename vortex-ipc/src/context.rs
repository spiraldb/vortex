use crate::flatbuffers::ipc as fb;
use crate::missing;
use vortex::array2::ViewContext;
use vortex::encoding::find_encoding;
use vortex_error::VortexError;

impl<'a> TryFrom<fb::Context<'a>> for ViewContext {
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
        Ok(Self::new(encodings.into()))
    }
}
