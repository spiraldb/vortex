# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Usage: python -I scan_iris.py NET,STA,LOC,CHA,START,END ...

Fetch many IRIS channels and compare per-chunk Delta/DoD vs LPC (model, bit-packed+patches)."""

import io
import os
import subprocess
import sys

import numpy as np
import obspy

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lpc_eval as ev


def fetch(net, sta, loc, cha, t0, t1):
    url = f"https://service.iris.edu/fdsnws/dataselect/1/query?net={net}&sta={sta}&loc={loc}&cha={cha}&starttime={t0}&endtime={t1}"
    b = subprocess.run(["curl", "-gsSL", "--fail", "-m", "300", url], capture_output=True).stdout
    if not b:
        return None
    st = obspy.read(io.BytesIO(b))
    st.merge(method=1, fill_value="interpolate")
    return max(st, key=lambda t: t.stats.npts).data.astype(np.int64)


def score(x):
    x = x[: (len(x) // 1024) * 1024][: 1 << 19]
    g = int(np.gcd.reduce(np.abs(x - x.min()))) or 1
    x = (x - x.min()) // g
    xc = ev.chunks(x)
    d = ev.patched_bits(ev.fixed_residual(xc, 1)[:, 1:]) / x.size
    dd = ev.patched_bits(ev.fixed_residual(xc, 2)[:, 2:]) / x.size
    best = 1e9
    for p in (8, 16, 32):
        hb, r, tail = ev.lpc_lanes_bits(x, p)
        best = min(best, (hb + ev.patched_bits(ev.chunks(r)) + (ev.for_bits(tail[None]) if len(tail) else 0)) / x.size)
        r2, h = ev.lpc_chunk_residual(xc, p)
        best = min(best, (ev.patched_bits(r2) + h.sum()) / x.size)
    return d, dd, best


for spec in sys.argv[1:]:
    net, sta, loc, cha, t0, t1 = spec.split(",")
    try:
        x = fetch(net, sta, loc, cha, t0, t1)
        if x is None or len(x) < 8192:
            print(spec, "no data")
            continue
        d, dd, lp = score(x)
        print(
            f"{net}.{sta}.{loc}.{cha} n={len(x)} delta {d:.2f} dod {dd:.2f} lpc {lp:.2f} gain vs best fixed {min(d, dd) / lp:.2f}x",
            flush=True,
        )
    except Exception as e:
        print(spec, "ERR", e)
