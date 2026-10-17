// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include <cstdint>

// Independent 16-byte loads each thread keeps in flight. Reads of host memory are latency bound:
// with one load per thread, an SM moves ~5 GB/s and saturating the host link takes two blocks on
// every SM; with eight, 16 blocks of 1024 threads saturate it (GH200: 334 GB/s), leaving the other
// SMs to decode kernels.
constexpr int MAPPED_COPY_LOADS = 8;

// Copies host memory that the device reads through the host page tables (for example a mapped
// file on a coherent CPU-GPU system) into device memory. Callers align source and target to
// the same offset within 16 bytes, so the body moves 16-byte words.
extern "C" __global__ void
mapped_copy(const uint8_t *__restrict__ source, uint8_t *__restrict__ target, uint64_t len) {
    const uint64_t thread = static_cast<uint64_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const uint64_t stride = static_cast<uint64_t>(gridDim.x) * blockDim.x;

    const uint64_t misalignment = reinterpret_cast<uintptr_t>(source) % 16;
    const uint64_t head = misalignment == 0 ? 0 : (16 - misalignment < len ? 16 - misalignment : len);
    for (uint64_t i = thread; i < head; i += stride) {
        target[i] = source[i];
    }

    const uint64_t words = (len - head) / 16;
    const uint4 *const source_words = reinterpret_cast<const uint4 *>(source + head);
    uint4 *const target_words = reinterpret_cast<uint4 *>(target + head);
    uint64_t i = thread;
    for (; i + (MAPPED_COPY_LOADS - 1) * stride < words; i += MAPPED_COPY_LOADS * stride) {
        uint4 loaded[MAPPED_COPY_LOADS];
#pragma unroll
        for (int k = 0; k < MAPPED_COPY_LOADS; k++) {
            loaded[k] = source_words[i + k * stride];
        }
#pragma unroll
        for (int k = 0; k < MAPPED_COPY_LOADS; k++) {
            target_words[i + k * stride] = loaded[k];
        }
    }
    for (; i < words; i += stride) {
        target_words[i] = source_words[i];
    }

    for (uint64_t i = head + words * 16 + thread; i < len; i += stride) {
        target[i] = source[i];
    }
}
