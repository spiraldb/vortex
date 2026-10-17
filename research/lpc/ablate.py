# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Ablations of the lane-parallel LPC encoder on the series where LPC wins.

For each series and order it reports bits/value (bit-packed with patches, as in lpc_eval.py) when:
- coefficients are fit on every lane, or on only 4 of the 64 lanes (a 1/16 sample);
- coefficients are quantized to 8, 12 or 15 signed bits;
and the largest accumulator magnitude seen, to check whether an i32 accumulator is exact.

Usage: python -I ablate.py <data_dir> [min_gain]
"""

import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lpc_eval as ev  # noqa: E402

LANES, RUN = 64, 1024


def quantize(coef, bits):
    cmax = max(np.abs(coef).max(), 1e-12)
    shift = int(np.clip(bits - 1 - np.ceil(np.log2(cmax + 1e-12)) - 1, 0, 30))
    return np.round(coef * 2.0**shift).astype(np.int64), shift


def lanes_bits(x, p, coef_bits=15, sample_lanes=LANES):
    m = (len(x) // RUN) * RUN
    x = x[:m] - int(np.round(x.mean()))
    runs = x.reshape(-1, RUN)
    total, res_all, max_acc, fit_s = 0.0, [], 0, 0.0
    for b in range(0, runs.shape[0], LANES):
        rb = runs[b : b + LANES]
        lag = ev.lag_matrix(rb, p)
        tgt = rb[:, p:]
        t = time.perf_counter()
        step = max(1, rb.shape[0] // sample_lanes)
        fl = lag[::step].reshape(-1, p).astype(np.float64)
        coef = ev.lpc_fit(fl[None], tgt[::step].reshape(-1)[None].astype(np.float64))[0]
        fit_s += time.perf_counter() - t
        q, shift = quantize(coef, coef_bits)
        acc = lag.reshape(-1, p) @ q + (1 << shift) // 2
        max_acc = max(max_acc, int(np.abs(acc).max()))
        res_all.append(tgt.reshape(-1) - (acc >> shift))
        starts = rb[:, 0]
        total += ev.width(np.array(starts.max() - starts.min())) * len(starts) + 72
        d = np.diff(rb[:, :p], axis=1)
        total += ev.width(np.array(d.max() - d.min())) * d.size + 8 + p * coef_bits + 6
    res = np.concatenate(res_all)
    k = (len(res) // ev.C) * ev.C
    total += ev.patched_bits(ev.chunks(res[:k]))
    if k < len(res):
        total += ev.for_bits(res[k:][None])
    return total / m, max_acc, fit_s / m * 1e9


if __name__ == "__main__":
    data = sys.argv[1]
    min_gain = float(sys.argv[2]) if len(sys.argv) > 2 else 1.05
    print(
        "| series | order | 15-bit, full fit | 15-bit, 1/16 fit | 12-bit | 8-bit | max abs accumulator (bits) | fit ns/value (full, 1/16) |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for line in open(os.path.join(data, "results.jsonl")):
        r = json.loads(line)
        base = min(r[f"{k}:patch"] for k in ("for", "delta", "dod", "affine"))
        keys = [f"lpc{p}/lanes:patch" for p in (4, 8, 16, 32)]
        best = min(keys, key=lambda k: r[k])
        if base / r[best] < min_gain:
            continue
        p = int(best[3:].split("/")[0])
        x = np.load(os.path.join(data, "series", r["name"] + ".npy"))[: 1 << 20].astype(np.int64)
        x = x[: (len(x) // ev.C) * ev.C]
        g = int(np.gcd.reduce(np.abs(x - x.min()))) or 1
        x = (x - x.min()) // g + x.min() // g
        full, acc, tf = lanes_bits(x, p)
        samp, _, ts = lanes_bits(x, p, sample_lanes=4)
        b12, _, _ = lanes_bits(x, p, coef_bits=12)
        b8, _, _ = lanes_bits(x, p, coef_bits=8)
        print(
            f"| {r['name']} | {p} | {full:.2f} | {samp:.2f} | {b12:.2f} | {b8:.2f} | "
            f"{np.log2(max(acc, 1)):.1f} | {tf:.0f}, {ts:.0f} |",
            flush=True,
        )
