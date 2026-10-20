// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! GPU filter implementation using CUB DeviceSelect::Flagged.

mod decimal;
mod primitive;
mod varbinview;

use std::fmt::Debug;

use async_trait::async_trait;

use tracing::instrument;
use vortex::array::ArrayRef;
use vortex::array::Canonical;
use vortex::array::arrays::Filter;
use vortex::array::arrays::filter::FilterArraySlotsExt;
use vortex::array::buffer::BufferHandle;
use vortex::array::match_each_decimal_value_type;
use vortex::array::match_each_native_simd_ptype;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::mask::Mask;
use vortex_cub::filter::CubFilterable;
use vortex_cuda_driver::DeviceRepr;

use crate::CudaBufferExt;
use crate::cub::select_bitmask;
use crate::executor::CudaArrayExt;
use crate::executor::CudaExecute;
use crate::executor::CudaExecutionCtx;
use crate::kernel::filter::decimal::filter_decimal;
use crate::kernel::filter::primitive::filter_primitive;
use crate::kernel::filter::varbinview::filter_varbinview;

/// CUDA executor for FilterArray using CUB DeviceSelect::Flagged.
#[derive(Debug)]
pub struct FilterExecutor;

#[async_trait]
impl CudaExecute for FilterExecutor {
    #[instrument(level = "trace", skip_all, fields(executor = ?self))]
    async fn execute(
        &self,
        array: ArrayRef,
        ctx: &mut CudaExecutionCtx,
    ) -> VortexResult<Canonical> {
        let filter_array = array
            .try_downcast::<Filter>()
            .map_err(|_| vortex_err!("Expected FilterArray"))?;

        let child = filter_array.child().clone();
        let mask = filter_array.data().filter_mask().clone();

        // Early return for trivial cases.
        match mask {
            Mask::AllTrue(_) => {
                // No data filtered => execute child without any post-processing
                child.execute_cuda(ctx).await
            }
            Mask::AllFalse(_) => {
                // All data filtered => empty canonical
                Ok(Canonical::empty(child.dtype()))
            }
            m @ Mask::Values(_) => {
                let canonical = child.execute_cuda(ctx).await?;
                match canonical {
                    Canonical::Primitive(prim) => {
                        match_each_native_simd_ptype!(prim.ptype(), |T| {
                            filter_primitive::<T>(prim, m, ctx).await
                        })
                    }
                    Canonical::Decimal(decimal) => {
                        match_each_decimal_value_type!(decimal.values_type(), |D| {
                            filter_decimal::<D>(decimal, m, ctx).await
                        })
                    }
                    Canonical::VarBinView(varbinview) => {
                        filter_varbinview(varbinview, m, ctx).await
                    }
                    c => vortex_bail!("CUDA filter kernel is not implemented for {}", c.dtype()),
                }
            }
        }
    }
}

async fn filter_sized<T: DeviceRepr + CubFilterable + Debug + Send + Sync + 'static>(
    input: BufferHandle,
    mask: Mask,
    ctx: &mut CudaExecutionCtx,
) -> VortexResult<BufferHandle> {
    let d_input = ctx.ensure_on_device(input).await?;

    // Construct the inputs for the cub::DeviceSelect::Flagged call.
    let output_len = mask.true_count();
    let (offset, len, flags) = mask.into_bit_buffer().into_inner();

    let d_flags = ctx.copy_to_device(flags.to_vec())?.await?;

    let input = d_input.cuda_view::<T>()?;
    let flags = d_flags.cuda_view::<u8>()?;
    // SAFETY: The uploaded mask and its true count describe this input range. Both inputs
    // are initialized, and select_bitmask records their use on the execution stream.
    unsafe { select_bitmask(input.slice(..len), &flags, offset, output_len, ctx) }
}
