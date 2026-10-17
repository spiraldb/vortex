# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Time pco compress/decompress per delta mode on real series (single thread, best of 7)."""

import sys
import time

import numpy as np
import pcodec
from pcodec import standalone

for name in sys.argv[2:]:
    x = np.load(f"{sys.argv[1]}/series/{name}.npy")[: 1 << 20].astype(np.int32)
    for label, spec in [
        ("consecutive", pcodec.DeltaSpec.auto()),
        ("conv1(8)", pcodec.DeltaSpec.try_conv1(8)),
        ("conv1(32)", pcodec.DeltaSpec.try_conv1(32)),
    ]:
        cfg = pcodec.ChunkConfig(compression_level=8, delta_spec=spec)
        t = time.perf_counter()
        c = standalone.simple_compress(x, cfg)
        ct = time.perf_counter() - t
        best = 1e9
        for _ in range(7):
            t = time.perf_counter()
            y = standalone.simple_decompress(c)
            best = min(best, time.perf_counter() - t)
        assert np.array_equal(y, x)
        print(
            f"{name:32s} {label:12s} {len(c) * 8 / len(x):6.2f} bits/value  compress {ct * 1e9 / len(x):6.1f} ns/value  decompress {best * 1e9 / len(x):5.2f} ns/value"
        )
