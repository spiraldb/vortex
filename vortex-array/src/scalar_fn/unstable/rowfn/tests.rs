// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Allocation conformance uses the existing in-crate tracking allocator.

use ::rowfn::{OutputBinding, OutputBuffer};
use ::rowfn::sink::{OutputSink, UninitElementSink};
use vortex_error::VortexResult;

use super::VortexHost;
use crate::{VortexSessionExecute, array_session};
use crate::arrays::Primitive;
use crate::memory::test_allocator::tracking_allocator;

#[test]
fn output_uses_execution_allocator_and_retains_primitive_allocation() -> VortexResult<()> {
    let (allocator, tracker) = tracking_allocator();
    let mut ctx = array_session().create_execution_ctx().with_allocator(allocator);
    let mut buffer = <VortexHost as OutputBinding<i64>>::allocate(3, &mut ctx)?;
    let slots = &mut buffer.slots()[..3];
    let pointer = slots.as_ptr().cast::<i64>();
    for (slot, value) in slots.iter_mut().zip([1, 2, 3]) { slot.write(value); }
    let mut buffer = buffer;
    assert_eq!(buffer.slots().as_ptr().cast::<i64>(), pointer);
    // SAFETY: the exact three published slots remain initialized across the move and second view.
    let output = unsafe { buffer.finish(3) };
    let primitive = output.as_::<Primitive>();
    let values = primitive.as_slice::<i64>();
    assert_eq!(values.as_ptr(), pointer);
    tracker.assert_owns(values);
    assert_eq!(tracker.live_allocations(), 1);
    drop(output);
    assert_eq!(tracker.live_allocations(), 0);
    Ok(())
}

#[test]
fn partially_initialized_sink_can_be_abandoned() -> VortexResult<()> {
    let (allocator, tracker) = tracking_allocator();
    let mut ctx = array_session().create_execution_ctx().with_allocator(allocator);
    let mut sink = UninitElementSink::<VortexHost, i64>::allocate(4, &(), &mut ctx)?;
    {
        let mut rows = sink.rows();
        // SAFETY: row zero is within the retained four-row view.
        let row = unsafe { UninitElementSink::<VortexHost, i64>::row(&mut rows, 0) };
        row.write(12);
    }
    drop(sink);
    assert_eq!(tracker.live_allocations(), 0);
    Ok(())
}
