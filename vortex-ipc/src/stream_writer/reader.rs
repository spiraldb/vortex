use std::sync::Arc;

use vortex::{Context, ViewContext};
use vortex_error::VortexResult;

use crate::array_stream::ArrayStream;
use crate::io::VortexRead;
use crate::MessageReader;

pub struct StreamArrayReader<R: VortexRead> {
    msgs: MessageReader<R>,
    view_context: Option<Arc<ViewContext>>,
}

impl<R: VortexRead> StreamArrayReader<R> {
    pub async fn try_new(read: R) -> VortexResult<Self> {
        Ok(Self {
            msgs: MessageReader::try_new(read).await?,
            view_context: None,
        })
    }

    /// Manually configure the view context.
    pub fn with_view_context(self, view_context: ViewContext) -> Self {
        assert!(self.view_context.is_none(), "View context already set");
        Self {
            view_context: Some(Arc::new(view_context)),
            ..self
        }
    }

    /// Load the view context from the stream.
    pub(crate) async fn load_view_context_with_default(mut self) -> VortexResult<Self> {
        self.load_view_context(&Context::default()).await
    }

    /// Load the view context from the stream.
    pub async fn load_view_context(mut self, ctx: &Context) -> VortexResult<Self> {
        assert!(self.view_context.is_none(), "View context already set");
        self.view_context = Some(self.msgs.read_view_context(ctx).await?);
        Ok(self)
    }

    /// Reads a single array from the stream.
    pub async fn array_stream(&mut self) -> VortexResult<impl ArrayStream + '_> {
        let view_context = self
            .view_context
            .as_ref()
            .expect("View context not set")
            .clone();

        let dtype = self.msgs.read_dtype().await?;

        Ok(self.msgs.array_stream(view_context, dtype))
    }
}
