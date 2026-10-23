// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use crate::CudaContext;
use crate::CudaStream;
use crate::DevicePtr;
use crate::DevicePtrMut;
use crate::DriverError;
use crate::sys;

type Streams = (Arc<CudaContext>, Arc<CudaStream>, Arc<CudaStream>);

/// A context and two streams, or `None` without a CUDA driver or device.
///
/// Fails if a device is present but cannot be set up, so tests do not pass without running.
fn streams() -> Result<Option<Streams>, DriverError> {
    // SAFETY: Only probes for the driver library.
    if !unsafe { sys::is_culib_present() } {
        return Ok(None);
    }
    match CudaContext::device_count() {
        Ok(0) | Err(DriverError(sys::CUresult::CUDA_ERROR_NO_DEVICE)) => return Ok(None),
        count => count?,
    };
    let context = CudaContext::new(0)?;
    let first = context.new_stream()?;
    let second = context.new_stream()?;
    Ok(Some((context, first, second)))
}

#[test]
fn host_device_round_trip() -> Result<(), DriverError> {
    let Some((context, stream, _)) = streams()? else {
        return Ok(());
    };
    let values: Vec<u32> = (0..100_000).collect();
    let device = stream.clone_htod(&values)?;
    assert_eq!(stream.clone_dtoh(&device)?, values);
    assert_eq!(stream.clone_dtoh(&device.slice(10..20))?, &values[10..20]);
    context.check_err()
}

/// A write on one stream is visible to reads on another, through views as well.
#[test]
fn cross_stream_reads_follow_writes() -> Result<(), DriverError> {
    let Some((context, writer, reader)) = streams()? else {
        return Ok(());
    };
    for round in 0..50u8 {
        let mut device = writer.alloc_zeros::<u8>(64 << 20)?;
        {
            let bytes = device.num_bytes();
            let (pointer, _record) = device.device_ptr_mut(&writer);
            // SAFETY: The allocation holds `bytes` bytes.
            unsafe { sys::cuMemsetD8Async(pointer, round, bytes, writer.cu_stream()) }.result()?;
        }
        let tail = reader.clone_dtoh(&device.slice((64 << 20) - 16..))?;
        assert!(tail.iter().all(|value| *value == round));
    }
    context.check_err()
}

/// A free after a read on another stream waits for that read, and the memory a later
/// allocation reuses is not overwritten under it.
#[test]
fn frees_follow_reads_on_other_streams() -> Result<(), DriverError> {
    let Some((context, owner, reader)) = streams()? else {
        return Ok(());
    };
    let source = owner.clone_htod(&vec![7u8; 32 << 20])?;
    let mut copies = Vec::new();
    for _ in 0..8 {
        // SAFETY: Fully written by the copy below.
        let mut target = unsafe { reader.alloc::<u8>(32 << 20) }?;
        reader.memcpy_dtod(&source, &mut target)?;
        copies.push(target);
    }
    drop(source);
    let reused = owner.clone_htod(&vec![9u8; 32 << 20])?;
    for copy in &copies {
        assert!(reader.clone_dtoh(copy)?.iter().all(|value| *value == 7));
    }
    assert!(owner.clone_dtoh(&reused)?.iter().all(|value| *value == 9));
    context.check_err()
}

/// Same-stream accesses take no events; cross-stream ones reuse returned events.
#[test]
fn events_are_pooled() -> Result<(), DriverError> {
    let Some((context, first, second)) = streams()? else {
        return Ok(());
    };
    for _ in 0..100 {
        let device = first.clone_htod(&[1u32; 1024])?;
        let (_pointer, record) = device.device_ptr(&second);
        drop(record);
    }
    // One write event and one read event, reused for every buffer.
    assert!(context.pooled_events() <= 2);
    context.check_err()
}

/// Empty allocations and copies succeed without driver calls the driver would reject.
#[test]
fn zero_length_allocations() -> Result<(), DriverError> {
    let Some((context, first, second)) = streams()? else {
        return Ok(());
    };
    let empty = first.clone_htod(&Vec::<u32>::new())?;
    assert!(empty.is_empty());
    let zeros = second.alloc_zeros::<u32>(0)?;
    assert!(second.clone_dtoh(&empty)?.is_empty());
    assert!(first.clone_dtoh(&zeros.try_clone()?)?.is_empty());
    drop((empty, zeros));
    context.check_err()
}

/// Reinterpreting more elements than fit is rejected, also when the byte count overflows.
#[test]
fn transmute_rejects_overflowing_lengths() -> Result<(), DriverError> {
    let Some((context, stream, _)) = streams()? else {
        return Ok(());
    };
    let mut device = stream.alloc_zeros::<u8>(16)?;
    // SAFETY: Only the lengths are checked; no views are used.
    unsafe {
        assert!(device.transmute::<u64>(2).is_some());
        assert!(device.transmute::<u64>(3).is_none());
        assert!(device.transmute::<u64>(usize::MAX / 4 + 1).is_none());
        assert!(
            device
                .as_view()
                .transmute::<u64>(usize::MAX / 4 + 1)
                .is_none()
        );
        assert!(device.transmute_mut::<u64>(usize::MAX / 4 + 1).is_none());
        assert!(
            device
                .as_view_mut()
                .transmute_mut::<u64>(usize::MAX / 4 + 1)
                .is_none()
        );
    }
    context.check_err()
}

/// Streams of another context are rejected instead of mixing the contexts' event pools.
#[test]
fn accesses_from_other_contexts_fail() -> Result<(), DriverError> {
    let Some((context, stream, _)) = streams()? else {
        return Ok(());
    };
    let other = CudaContext::new_non_primary(0, 0)?;
    let foreign = other.new_stream()?;
    let device = stream.clone_htod(&[1u32; 16])?;
    let pooled = context.pooled_events();
    let error = foreign.clone_dtoh(&device).expect_err("cross-context read");
    assert_eq!(
        error,
        DriverError(sys::CUresult::CUDA_ERROR_INVALID_CONTEXT)
    );
    assert_eq!(other.pooled_events(), 0);
    assert_eq!(context.pooled_events(), pooled);
    drop(device);
    // The read's guard records the rejection too, after the copy returned it.
    assert_eq!(
        other.check_err(),
        Err(DriverError(sys::CUresult::CUDA_ERROR_INVALID_CONTEXT))
    );
    context.check_err()
}

/// The first error recorded is the one reported.
#[test]
fn record_err_keeps_the_first_error() -> Result<(), DriverError> {
    let Some((context, ..)) = streams()? else {
        return Ok(());
    };
    context.record_err(Err::<(), _>(DriverError(
        sys::CUresult::CUDA_ERROR_NO_DEVICE,
    )));
    context.record_err(Err::<(), _>(DriverError(
        sys::CUresult::CUDA_ERROR_INVALID_CONTEXT,
    )));
    assert_eq!(
        context.check_err(),
        Err(DriverError(sys::CUresult::CUDA_ERROR_NO_DEVICE))
    );
    context.check_err()
}
