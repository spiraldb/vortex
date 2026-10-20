// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! CUDA stream utility functions.

use std::fmt::Debug;
use std::mem::size_of;
use std::mem::size_of_val;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::mpsc;

use futures::future::BoxFuture;
use kanal::Sender;
use vortex::array::buffer::BufferHandle;
use vortex::error::VortexResult;
use vortex::error::vortex_ensure;
use vortex::error::vortex_err;
use vortex_cuda_driver::CudaEvent;
use vortex_cuda_driver::CudaSlice;
use vortex_cuda_driver::CudaStream;
use vortex_cuda_driver::DeviceRepr;
use vortex_cuda_driver::ValidAsZeroBits;

use crate::CudaDeviceBuffer;
use crate::device_buffer::CUDF_VALIDITY_BUFFER_PADDING;

// cuDF imports Arrow validity masks into padded buffers and kernels may read through that
// padded extent. Keep copied device buffers padded and zero-tailed so Arrow validity exports
// can safely reuse matching bitmaps without repacking.

#[derive(Clone)]
pub struct VortexCudaStream(pub(crate) Arc<CudaStream>);

impl Deref for VortexCudaStream {
    type Target = Arc<CudaStream>;

    fn deref(&self) -> &Arc<CudaStream> {
        &self.0
    }
}

impl VortexCudaStream {
    /// Allocates a typed buffer on the GPU.
    ///
    /// Note: Allocation is async in case the CUDA driver supports this.
    ///
    /// The condition for alloc to be async is support for memory pools:
    /// `CU_DEVICE_ATTRIBUTE_MEMORY_POOLS_SUPPORTED`.
    ///
    /// Any kernel submitted to the stream after alloc can safely use the
    /// memory, as operations on the stream are ordered sequentially.
    pub(crate) fn device_alloc<T: DeviceRepr + Send + Sync + 'static>(
        &self,
        len: usize,
    ) -> VortexResult<CudaSlice<T>> {
        // SAFETY: No safety guarantees for allocations on the GPU.
        unsafe {
            self.alloc::<T>(len)
                .map_err(|e| vortex_err!("Failed to allocate device memory: {}", e))
        }
    }

    /// Allocates bitmap bytes with cuDF-sized padding, zeroing only the tail.
    ///
    /// The producer must initialize `byte_count` bytes on this stream and leave the padding untouched.
    pub(crate) fn device_alloc_bitmap(&self, byte_count: usize) -> VortexResult<CudaSlice<u8>> {
        let allocation_len = padded_device_allocation_len::<u8>(byte_count)?;
        let mut buffer = self.device_alloc::<u8>(allocation_len)?;
        zero_padding(self, &mut buffer, byte_count)?;
        Ok(buffer)
    }

    /// Copies host data to the device.
    ///
    /// Allocates device memory, schedules an async copy, and returns a future
    /// that completes when the copy is finished. The source data is moved into
    /// the future to ensure it remains valid until the copy completes.
    ///
    /// For **pageable** host memory, `memcpy_htod_async` stages the source
    /// synchronously before returning. For **pinned** host memory the transfer
    /// is truly async and the source must stay alive until the copy completes
    /// (guaranteed by the returned future capturing it).
    ///
    /// The returned [`BufferHandle`] keeps the source byte length, while its
    /// CUDA allocation may include zeroed tail padding for consumers such as cuDF
    /// that read validity masks through padded extents.
    pub(crate) fn copy_to_device<T, D>(
        &self,
        data: D,
    ) -> VortexResult<BoxFuture<'static, VortexResult<BufferHandle>>>
    where
        T: DeviceRepr + ValidAsZeroBits + Debug + Send + Sync + 'static,
        D: AsRef<[T]> + Send + 'static,
    {
        let host_slice: &[T] = data.as_ref();
        let byte_count = size_of_val(host_slice);
        let allocation_len = padded_device_allocation_len::<T>(byte_count)?;
        // `device_alloc` binds the CUDA context to the current thread.
        let mut cuda_slice: CudaSlice<T> = self.device_alloc::<T>(allocation_len)?;

        let mut values = cuda_slice.slice_mut(..host_slice.len());
        self.memcpy_htod(host_slice, &mut values)
            .map_err(|e| vortex_err!("Failed to schedule H2D copy: {}", e))?;

        zero_padding(self, &mut cuda_slice, host_slice.len())?;

        // `zero_padding` zeroed all allocation bytes after `byte_count`.
        let cuda_buf = CudaDeviceBuffer::new_with_zeroed_tail(cuda_slice, byte_count)?;
        let buffer = BufferHandle::new_device(Arc::new(cuda_buf)).slice(0..byte_count);
        let stream = Arc::clone(&self.0);

        Ok(Box::pin(async move {
            await_stream_callback(&stream).await?;

            // Keep source memory alive until copy completes.
            let _keep_alive = data;

            Ok(buffer)
        }))
    }

    /// Synchronous variant of [`copy_to_device`](Self::copy_to_device).
    ///
    /// Allocates device memory, enqueues the H2D copy on the stream, and
    /// returns immediately. The device pointer is valid as soon as this call
    /// returns; the copy completes before any later work on the same stream.
    ///
    /// For **pageable** host memory (the common case), `memcpy_htod` stages
    /// the source into a driver-managed pinned buffer before returning, so
    /// the source data is safe to drop after this call.
    ///
    /// Like [`copy_to_device`](Self::copy_to_device), this preserves the source
    /// byte length on the returned handle while keeping any tail padding in the
    /// backing CUDA allocation.
    pub(crate) fn copy_to_device_sync<T>(&self, data: &[T]) -> VortexResult<BufferHandle>
    where
        T: DeviceRepr + ValidAsZeroBits + Debug + Send + Sync + 'static,
    {
        let byte_count = size_of_val(data);
        let allocation_len = padded_device_allocation_len::<T>(byte_count)?;
        let mut cuda_slice: CudaSlice<T> = self.device_alloc(allocation_len)?;

        let mut values = cuda_slice.slice_mut(..data.len());
        self.memcpy_htod(data, &mut values)
            .map_err(|e| vortex_err!("Failed to schedule H2D copy: {}", e))?;

        zero_padding(self, &mut cuda_slice, data.len())?;

        // `zero_padding` zeroed all allocation bytes after `byte_count`.
        let cuda_buf = CudaDeviceBuffer::new_with_zeroed_tail(cuda_slice, byte_count)?;
        Ok(BufferHandle::new_device(Arc::new(cuda_buf)).slice(0..byte_count))
    }
}

/// Returns the typed CUDA allocation length for `byte_count`.
///
/// The backing allocation is padded for consumers such as cuDF that read validity masks
/// through padded extents. The returned length is in `T` elements.
fn padded_device_allocation_len<T>(byte_count: usize) -> VortexResult<usize> {
    let element_size = size_of::<T>();
    vortex_ensure!(
        element_size != 0,
        "cannot copy zero-sized values to CUDA device"
    );
    let min_allocation_bytes = byte_count.next_multiple_of(CUDF_VALIDITY_BUFFER_PADDING);
    Ok(min_allocation_bytes.div_ceil(element_size))
}

/// Zeroes the allocation tail after `initialized_count` elements.
///
/// Copies or kernels must initialize the preceding elements on the same stream.
/// cuDF reads bitmaps in whole mask words, which can extend past the final logical byte.
/// Allocations include tail padding for those reads; this zeroes that padding.
pub(crate) fn zero_padding<T: DeviceRepr + ValidAsZeroBits>(
    stream: &VortexCudaStream,
    cuda_slice: &mut CudaSlice<T>,
    initialized_count: usize,
) -> VortexResult<()> {
    if initialized_count >= cuda_slice.len() {
        return Ok(());
    }

    let mut padding = cuda_slice.slice_mut(initialized_count..);
    stream
        .memset_zeros(&mut padding)
        .map_err(|e| vortex_err!("Failed to zero device buffer padding: {}", e))
}

/// Asynchronously waits for the work queued on `stream` so far to complete.
///
/// This function can be used to asynchronously wait for events previously
/// submitted to the stream to complete, e.g. async buffer allocations.
///
/// Note: This is not equivalent to calling sync on a stream: work queued after
/// this call is not awaited.
///
/// # Arguments
///
/// * `stream` - The CUDA stream to wait on
///
/// # Errors
///
/// Returns an error if recording the completion fails, if the awaited work
/// failed, or if the completion channel closes unexpectedly.
pub(crate) async fn await_stream_callback(stream: &CudaStream) -> VortexResult<()> {
    let rx = register_stream_callback(stream)?;

    rx.recv()
        .await
        .map_err(|e| vortex_err!("CUDA stream completion channel closed unexpectedly: {}", e))?
}

/// A future's wait for an event, polled by the completion thread.
struct Completion {
    event: CudaEvent,
    done: Sender<VortexResult<()>>,
}

/// Completions of every stream, polled by one thread.
///
/// Host functions (`cuLaunchHostFunc`) would deliver completions without polling, but enqueueing
/// one takes a driver lock that pageable copies hold for their whole duration, while querying an
/// event does not wait for it. Recording uses pooled events, so it creates none either.
static COMPLETIONS: LazyLock<Option<mpsc::Sender<Completion>>> = LazyLock::new(|| {
    let (sender, receiver) = mpsc::channel::<Completion>();
    std::thread::Builder::new()
        .name("vortex-cuda-completion".to_string())
        .spawn(move || poll_completions(&receiver))
        .ok()
        .map(|_| sender)
});

/// Reports each event's completion to its future, in any order.
///
/// Spins while events are pending, so completions wake futures within microseconds, and sleeps
/// on the channel otherwise. Runs until the process exits: the sender is a process-wide static.
fn poll_completions(receiver: &mpsc::Receiver<Completion>) {
    let mut pending: Vec<Completion> = Vec::new();
    loop {
        if pending.is_empty() {
            match receiver.recv() {
                Ok(completion) => pending.push(completion),
                Err(_) => return,
            }
        }
        pending.extend(receiver.try_iter());
        pending.retain(|completion| {
            let result = match completion.event.try_is_complete() {
                Ok(false) => return true,
                Ok(true) => Ok(()),
                Err(error) => Err(vortex_err!("CUDA stream work failed: {error}")),
            };
            // A dropped receiver means the future was cancelled; nobody waits for the result.
            drop(completion.done.send(result));
            false
        });
        if !pending.is_empty() {
            std::thread::yield_now();
        }
    }
}

/// Registers a completion for the work queued on `stream` so far.
///
/// # Returns
///
/// An async receiver that receives the result once all preceding work on the stream completes.
///
/// # Errors
///
/// Returns an error if recording the event fails.
fn register_stream_callback(
    stream: &CudaStream,
) -> VortexResult<kanal::AsyncReceiver<VortexResult<()>>> {
    let event = stream
        .record_event(None)
        .map_err(|e| vortex_err!("Failed to record CUDA stream completion: {}", e))?;
    let (done, receiver) = kanal::bounded::<VortexResult<()>>(1);
    let completion = Completion { event, done };
    match COMPLETIONS.as_ref() {
        Some(completions) => completions
            .send(completion)
            .map_err(|_| vortex_err!("CUDA completion thread stopped"))?,
        // Without the completion thread, wait here.
        None => {
            let result = completion
                .event
                .synchronize()
                .map_err(|e| vortex_err!("CUDA stream work failed: {}", e));
            drop(completion.done.send(result));
        }
    }
    Ok(receiver.to_async())
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use vortex::array::Canonical;
    use vortex::array::IntoArray;
    use vortex::array::VortexSessionExecute;
    use vortex::array::arrays::BoolArray;
    use vortex::array::assert_arrays_eq;
    use vortex::array::validity::Validity;
    use vortex::error::VortexResult;

    use super::padded_device_allocation_len;
    use crate::CanonicalCudaExt;
    use crate::CudaSession;
    use crate::device_buffer::CUDF_VALIDITY_BUFFER_PADDING;
    use crate::device_buffer::cuda_backing_allocation;

    #[test]
    fn test_padded_device_allocation_len() -> VortexResult<()> {
        assert_eq!(padded_device_allocation_len::<u8>(0)?, 0);
        assert_eq!(
            padded_device_allocation_len::<u8>(1)?,
            CUDF_VALIDITY_BUFFER_PADDING
        );
        assert_eq!(
            padded_device_allocation_len::<u8>(4)?,
            CUDF_VALIDITY_BUFFER_PADDING
        );
        assert_eq!(
            padded_device_allocation_len::<u8>(5)?,
            CUDF_VALIDITY_BUFFER_PADDING
        );
        assert_eq!(
            padded_device_allocation_len::<u32>(1)?,
            CUDF_VALIDITY_BUFFER_PADDING / size_of::<u32>()
        );
        assert_eq!(
            padded_device_allocation_len::<u32>(5)?,
            CUDF_VALIDITY_BUFFER_PADDING / size_of::<u32>()
        );
        Ok(())
    }

    #[crate::test]
    async fn test_copy_to_device_preserves_visible_len_with_padding() -> VortexResult<()> {
        let ctx = CudaSession::create_execution_ctx(&crate::cuda_session())?;
        let handle = ctx.stream().copy_to_device(vec![0xab_u8])?.await?;

        assert_eq!(handle.len(), 1);
        let host = handle.try_to_host()?.await?;
        assert_eq!(host.as_slice(), &[0xab]);

        let backing = cuda_backing_allocation(&handle)?;
        assert_eq!(backing.len(), CUDF_VALIDITY_BUFFER_PADDING);
        let backing_host = backing.try_to_host()?.await?;
        assert_eq!(backing_host[0], 0xab);
        assert!(backing_host[1..].iter().all(|byte| *byte == 0));

        Ok(())
    }

    #[crate::test]
    async fn test_copy_to_device_sync_preserves_visible_len_with_padding() -> VortexResult<()> {
        let ctx = CudaSession::create_execution_ctx(&crate::cuda_session())?;
        let handle = ctx.stream().copy_to_device_sync(&[1_u8, 2, 3, 4, 5])?;

        assert_eq!(handle.len(), 5);
        let host = handle.try_to_host()?.await?;
        assert_eq!(host.as_slice(), &[1, 2, 3, 4, 5]);

        let backing = cuda_backing_allocation(&handle)?;
        assert_eq!(backing.len(), CUDF_VALIDITY_BUFFER_PADDING);
        let backing_host = backing.try_to_host()?.await?;
        assert_eq!(&backing_host[..5], &[1, 2, 3, 4, 5]);
        assert!(backing_host[5..].iter().all(|byte| *byte == 0));

        Ok(())
    }

    #[crate::test]
    async fn test_slice_device_bool_preserves_device_buffer() -> VortexResult<()> {
        let ctx = CudaSession::create_execution_ctx(&crate::cuda_session())?;
        let bits = ctx
            .stream()
            .copy_to_device(vec![0b1010_1100_u8, 0b0110_1001, 0b1100_0011])?
            .await?;
        let array = BoolArray::new_handle(bits, 3, 18, Validity::NonNullable).into_array();

        let sliced = array.slice(7..16)?;

        assert_eq!(sliced.len(), 9);
        assert!(sliced.buffer_handles()[0].is_on_device());
        let mut cpu_ctx = vortex_array::array_session().create_execution_ctx();
        let host = sliced
            .execute::<Canonical>(&mut cpu_ctx)?
            .into_host(&mut cpu_ctx)
            .await?;
        assert_arrays_eq!(
            host.into_array(),
            BoolArray::from_iter([false, true, false, true, true, false, true, true, false]),
            &mut cpu_ctx
        );
        Ok(())
    }
}
