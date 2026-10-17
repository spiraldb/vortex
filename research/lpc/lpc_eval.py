# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Estimate compressed bits/value of FoR, Delta, delta-of-delta, Affine and LPC on integer series.

Every lightweight scheme codes residuals in 1024-value chunks with per-chunk frame of reference and
bit width (FoR + VarBitPacked), so they differ only in the predictor. Pco and zstd are references.

Usage: python -I lpc_eval.py <data_dir> [name filter]
"""

import json
import math
import os
import sys
import time

import numpy as np
import pcodec
import zstandard
from pcodec import standalone

C = 1024
COEF_BITS = 15  # signed fixed-point coefficient width, FLAC's default precision


def width(span):
    return np.where(span > 0, np.floor(np.log2(np.maximum(span, 1))).astype(np.int64) + 1, 0)


def for_bits(res, header_bits=0):
    """Bits to store residual chunks (rows) with per-chunk min + bit width."""
    span = res.max(axis=1) - res.min(axis=1)
    return float((width(span) * res.shape[1] + 64 + 8 + header_bits).sum())


def patched_bits(res, header_bits=0):
    """Like for_bits but each chunk may pick a narrower width and patch outliers (u16 index + value)."""
    lo = res.min(axis=1, keepdims=True)
    u = res - lo
    full = width(u.max(axis=1))
    srt = np.sort(u, axis=1)
    n = res.shape[1]
    best = full * n
    for w in range(0, int(full.max()) + 1):
        lim = (1 << w) - 1 if w < 63 else np.iinfo(np.int64).max
        exc = n - (srt <= lim).sum(axis=1)
        cost = w * n + exc * (16 + full)
        best = np.minimum(best, cost)
    total = (best + 64 + 8 + 8 + header_bits).sum()
    return float(total)


def chunks(x):
    m = len(x) // C
    return x[: m * C].reshape(m, C)


def fixed_residual(xc, order):
    """FLAC fixed predictor of the given order within each chunk; first `order` values become deltas."""
    r = xc.copy()
    for _ in range(order):
        r[:, 1:] = r[:, 1:] - r[:, :-1].copy()
    return r


def warmup_bits(xc, p):
    """Store the first p values of each chunk: first value is the chunk reference, the rest as deltas."""
    if p <= 1:
        return np.zeros(xc.shape[0])
    d = np.diff(xc[:, :p], axis=1)
    return width(d.max(axis=1) - d.min(axis=1)) * (p - 1) + 8


def lpc_fit(design, target, ridge=1e-6):
    """Batched least squares (covariance method): design (b, m, p), target (b, m) -> (b, p)."""
    a = np.einsum("bmi,bmj->bij", design, design)
    rhs = np.einsum("bmi,bm->bi", design, target)
    tr = np.trace(a, axis1=1, axis2=2)[:, None, None] / a.shape[1]
    a = a + (ridge * tr + 1e-9) * np.eye(a.shape[1])
    return np.linalg.solve(a, rhs[..., None])[..., 0]


def quantize(coef):
    """FLAC-style: shift chosen so the largest coefficient fits COEF_BITS signed bits."""
    cmax = np.abs(coef).max(axis=-1, keepdims=True)
    cmax = np.maximum(cmax, 1e-12)
    shift = np.clip(COEF_BITS - 1 - np.ceil(np.log2(cmax + 1e-12)).astype(np.int64) - 1, 0, 30)
    q = np.round(coef * (2.0**shift)).astype(np.int64)
    return q, shift


def lag_matrix(xc, p):
    """For chunk rows xc (b, C), the (b, C-p, p) matrix of x[i-1..i-p] for i in p..C."""
    cols = [xc[:, p - k : C - k] for k in range(1, p + 1)]
    return np.stack(cols, axis=2)


def lpc_chunk_residual(xc, p):
    """Per-chunk LPC: coefficients fit and stored per chunk, first p values are warm-ups."""
    mean = np.round(xc.mean(axis=1, keepdims=True)).astype(np.int64)
    z = xc - mean
    lag = lag_matrix(z, p)
    tgt = z[:, p:]
    coef = lpc_fit(lag.astype(np.float64), tgt.astype(np.float64))
    q, shift = quantize(coef)
    pred = (np.einsum("bmp,bp->bm", lag, q) + (1 << shift) // 2) >> shift
    res = tgt - pred
    header = p * COEF_BITS + 5 + 6 + warmup_bits(xc, p)
    return res, header


def lpc_global_residual(x, p, block=1 << 16):
    """LPC with one coefficient set per 64K-value block, predicted across chunk boundaries."""
    x = x - int(np.round(x.mean()))
    res = np.empty(len(x) - p, dtype=np.int64)
    coef_bits = 0
    for s in range(p, len(x), block):
        e = min(s + block, len(x))
        idx = np.arange(s, e)
        lag = np.stack([x[idx - k] for k in range(1, p + 1)], axis=1)
        tgt = x[s:e]
        coef = lpc_fit(lag[None].astype(np.float64), tgt[None].astype(np.float64))[0]
        q, shift = quantize(coef[None])
        q, shift = q[0], int(shift.item())
        pred = (lag @ q + (1 << shift) // 2) >> shift
        res[s - p : e - p] = tgt - pred
        coef_bits += p * COEF_BITS + 6
    return res, coef_bits


def lpc_lanes_bits(x, p, lanes=64, run=1024):
    """LPC with one coefficient set per lanes*run block, decoded as `lanes` independent runs.

    Each run of `run` contiguous values is one SIMD lane, so its first p values cannot be predicted
    from the previous run and are stored as warm-ups: the first as FoR over the block's run starts,
    the rest as in-run deltas.
    """
    m = (len(x) // run) * run
    x = x[:m] - int(np.round(x.mean()))
    runs = x.reshape(-1, run)
    total = 0.0
    res_all = []
    for b in range(0, runs.shape[0], lanes):
        rb = runs[b : b + lanes]
        lag = lag_matrix(rb, p).reshape(-1, p)
        tgt = rb[:, p:].reshape(-1)
        coef = lpc_fit(lag[None].astype(np.float64), tgt[None].astype(np.float64))[0]
        q, shift = quantize(coef[None])
        q, shift = q[0], int(shift.item())
        pred = (lag @ q + (1 << shift) // 2) >> shift
        res_all.append((tgt - pred).reshape(rb.shape[0], run - p))
        starts = rb[:, 0]
        total += width(np.array(starts.max() - starts.min())) * len(starts) + 64 + 8
        if p > 1:
            d = np.diff(rb[:, :p], axis=1)
            total += width(np.array(d.max() - d.min())) * d.size + 8
        total += p * COEF_BITS + 6
    res = np.concatenate([r.reshape(-1) for r in res_all])
    k = (len(res) // C) * C
    return total, res[:k], res[k:]


def affine_residual(xc):
    j = np.arange(C, dtype=np.float64)
    jm = j - j.mean()
    slope = ((xc - xc.mean(axis=1, keepdims=True)) * jm).sum(axis=1) / (jm**2).sum()
    fx = np.round(slope * 65536).astype(np.int64)  # 16-bit fractional fixed point slope
    pred = (fx[:, None] * np.arange(C)[None, :]) >> 16
    return xc - pred, 64


def as_i32(x):
    if np.iinfo(np.int32).min <= x.min() and x.max() <= np.iinfo(np.int32).max:
        return x.astype(np.int32)
    return x


def pco_bits(x, delta):
    cfg = pcodec.ChunkConfig(compression_level=8, delta_spec=delta)
    return len(standalone.simple_compress(as_i32(x), cfg)) * 8


def narrow(x):
    lo, hi = x.min(), x.max()
    for t in (np.int8, np.int16, np.int32):
        if np.iinfo(t).min <= lo and hi <= np.iinfo(t).max:
            return x.astype(t)
    return x


def predictors(x, xc):
    """Yield (name, residual chunks (b, m), extra header bits) for every predictor."""
    yield "for", xc, 0.0
    yield "delta", fixed_residual(xc, 1)[:, 1:], 0.0
    yield "dod", fixed_residual(xc, 2)[:, 2:], float(warmup_bits(xc, 2).sum())
    r, h = affine_residual(xc)
    yield "affine", r, float(h * xc.shape[0])
    for p in (2, 4, 8, 16, 32):
        r, h = lpc_chunk_residual(xc, p)
        yield f"lpc{p}/chunk", r, float(np.sum(h))
    for p in (2, 4, 8, 16, 32):
        r, cb = lpc_global_residual(x, p)
        r = np.concatenate([np.zeros(p, dtype=np.int64), r])
        yield f"lpc{p}/block", chunks(r), float(cb + 64 * p)
    for p in (4, 8, 16, 32):
        hb, r, tail = lpc_lanes_bits(x, p)
        tail_bits = for_bits(tail[None]) if len(tail) else 0.0
        yield f"lpc{p}/lanes", chunks(r), float(hb + tail_bits)


def evaluate(x):
    x = x.astype(np.int64)
    x = x[: (len(x) // C) * C]
    g = int(np.gcd.reduce(np.abs(x - x.min()))) or 1
    x = (x - x.min()) // g + x.min() // g
    xc = chunks(x)
    n = x.size
    out = {"gcd": g}
    for name, r, h in predictors(x, xc):
        out[name + ":bp"] = (for_bits(r) + h) / n
        out[name + ":patch"] = (patched_bits(r) + h) / n
        out[name + ":ans"] = (pco_bits(r.ravel(), pcodec.DeltaSpec.no_op()) + h) / n
    # FLAC-style fixed order 0..4 chosen per chunk, bit-packed
    best = None
    for o in range(5):
        r = fixed_residual(xc, o)[:, o:]
        b = width(r.max(axis=1) - r.min(axis=1)) * r.shape[1] + 72 + 3 + warmup_bits(xc, o)
        best = b if best is None else np.minimum(best, b)
    out["fixed0-4:bp"] = best.sum() / n
    for coder in ("bp", "patch", "ans"):
        for kind in ("chunk", "block", "lanes"):
            ks = [f"lpc{p}/{kind}:{coder}" for p in ((4, 8, 16, 32) if kind == "lanes" else (2, 4, 8, 16, 32))]
            out[f"lpc-best/{kind}:{coder}"] = min(out[k] for k in ks)

    out["pco-auto"] = pco_bits(x, pcodec.DeltaSpec.auto()) / n
    out["pco-consec"] = min(pco_bits(x, pcodec.DeltaSpec.try_consecutive(o)) for o in (1, 2, 3)) / n
    conv = {}
    for o in (2, 4, 8, 16, 32):
        try:
            conv[o] = pco_bits(x, pcodec.DeltaSpec.try_conv1(o)) / n
        except Exception:  # noqa: BLE001
            pass
    out["pco-conv1"] = min(conv.values()) if conv else math.nan
    out["pco-conv1-order"] = min(conv, key=conv.get) if conv else 0
    out["zstd3"] = len(zstandard.ZstdCompressor(level=3).compress(narrow(x).tobytes())) * 8 / n
    out["zstd19"] = len(zstandard.ZstdCompressor(level=19).compress(narrow(x).tobytes())) * 8 / n
    return out


LIGHT = ["for:patch", "delta:patch", "dod:patch", "affine:patch"]
LPCS = ["lpc-best/chunk:patch", "lpc-best/block:patch"]

if __name__ == "__main__":
    data = sys.argv[1]
    flt = sys.argv[2] if len(sys.argv) > 2 else ""
    rows = [json.loads(line) for line in open(os.path.join(data, "manifest.jsonl"))]
    seen = {}
    for r in rows:
        seen[r["name"]] = r
    results_path = os.path.join(data, "results.jsonl")
    done = set()
    if os.path.exists(results_path):
        done = {json.loads(line)["name"] for line in open(results_path)}
    for r in seen.values():
        if flt not in r["name"] or r["name"] in done:
            continue
        x = np.load(os.path.join(data, "series", r["name"] + ".npy"))
        if len(x) < 4 * C:
            print("skip short", r["name"])
            continue
        x = x[: 1 << 20]
        t = time.time()
        res = evaluate(x)
        res.update(r)
        res["n_eval"] = int((len(x) // C) * C)
        with open(results_path, "a") as f:
            f.write(json.dumps(res) + "\n")
        bl = min(res[k] for k in LIGHT)
        bk = min(LIGHT, key=lambda k: res[k])
        lp = min(res[k] for k in LPCS)
        print(
            f"{r['name']:38s} best-light {bk:8s} {bl:6.2f}  lpc {lp:6.2f}  pco-auto {res['pco-auto']:6.2f} "
            f"consec {res['pco-consec']:6.2f} conv1 {res['pco-conv1']:6.2f}  gain {bl / lp:5.2f}x  ({time.time() - t:.0f}s)",
            flush=True,
        )
