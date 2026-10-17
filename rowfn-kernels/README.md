<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- SPDX-FileCopyrightText: Copyright the Vortex contributors -->

# rowfn-kernels

This unpublished crate contains the buffer-independent lane sources, out-of-place traversal, and
Boolean packing extracted from Vortex. It has no Arrow or Vortex dependencies. Callers retain storage
ownership and supply writable slots or words.

`IndexedSource` is unsafe to implement because collectors and zipped sources rely on stable lengths
and valid in-bounds reads. Its slice and zip implementations retain those invariants.

`BitmapView` borrows bytes with a bit offset and logical length. It supports unaligned slices and
reads only the bytes that cover the requested range. It requires no padding or owned-buffer wrapper.
Vortex's corresponding entry points delegate to these shared implementations.

The Boolean tail stays inlined, and multiversioned packing retains its separate entry point. These
source constraints come from the existing implementation. The repository's earlier test and
measurement reports concern their recorded source revisions. They do not isolate the kernel move
from the executor changes. No tests or compiler experiments have run for the cleanup.
