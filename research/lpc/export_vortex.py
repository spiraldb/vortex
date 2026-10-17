# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Write each series, its best LPC residuals and its Delta / delta-of-delta residuals as raw i64
columns for `affine_eval`.

The `affine_eval` example on `ji/affine-encoding` compresses each column with real BtrBlocks
configurations. Comparing the original column with the LPC residual column (plus the LPC header
bits listed in lpc_headers.tsv) gives real Vortex sizes instead of the model in lpc_eval.py.

Usage: python -I export_vortex.py <data_dir> <out_dir>
"""

import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lpc_eval as ev  # noqa: E402

data, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
results = {}
for line in open(os.path.join(data, "results.jsonl")):
    r = json.loads(line)
    results[r["name"]] = r

manifest = open(os.path.join(out, "manifest.tsv"), "w")
headers = open(os.path.join(out, "lpc_headers.tsv"), "w")
fixed = open(os.path.join(out, "fixed_headers.tsv"), "w")
for name, r in results.items():
    keys = [f"lpc{p}/lanes:patch" for p in (4, 8, 16, 32)] + [f"lpc{p}/chunk:patch" for p in (2, 4, 8, 16, 32)]
    best = min(keys, key=lambda k: r[k])
    p = int(best[3:].split("/")[0])
    kind = best.split("/")[1].split(":")[0]

    x = np.load(os.path.join(data, "series", name + ".npy"))[: 1 << 20].astype(np.int64)
    x = x[: (len(x) // ev.C) * ev.C]
    g = int(np.gcd.reduce(np.abs(x - x.min()))) or 1
    x = (x - x.min()) // g + x.min() // g

    if kind == "lanes":
        header, res, tail = ev.lpc_lanes_bits(x, p)
        res = np.concatenate([res, tail])
    else:
        r2, h = ev.lpc_chunk_residual(ev.chunks(x), p)
        res, header = r2.ravel(), float(np.sum(h))

    # Fixed predictors, for a like-for-like cascade: per-chunk deltas (first value of each chunk is a
    # header, FoR-coded across chunks) and delta-of-delta (two warm-ups per chunk).
    xc = ev.chunks(x)
    firsts = xc[:, 0]
    first_bits = float(ev.width(np.array(firsts.max() - firsts.min())) * len(firsts) + 72)
    delta = ev.fixed_residual(xc, 1)[:, 1:].ravel()
    dod = ev.fixed_residual(xc, 2)[:, 2:].ravel()
    dod_bits = first_bits + float(ev.warmup_bits(xc, 2).sum())
    delta.astype("<i8").tofile(os.path.join(out, f"{name}__delta.i64"))
    dod.astype("<i8").tofile(os.path.join(out, f"{name}__dod.i64"))
    manifest.write(f"{name}__delta\t{len(delta)}\n{name}__dod\t{len(dod)}\n")
    fixed.write(f"{name}\t{len(x)}\t{first_bits:.0f}\t{dod_bits:.0f}\n")

    x.astype("<i8").tofile(os.path.join(out, f"{name}.i64"))
    res.astype("<i8").tofile(os.path.join(out, f"{name}__lpc.i64"))
    manifest.write(f"{name}\t{len(x)}\n{name}__lpc\t{len(res)}\n")
    headers.write(f"{name}\t{len(x)}\t{best}\t{header:.0f}\n")
    print(name, best, len(x), len(res))
