# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Per-note LPC vs Delta/delta-of-delta gain over every acoustic NSynth test note, by pitch band.

Usage: python -I nsynth_notes.py <path to nsynth-test.jsonwav.tar.gz>
"""

import collections
import io
import os
import sys
import tarfile

import numpy as np
import soundfile as sf

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lpc_eval as ev

rows = []
with tarfile.open(sys.argv[1]) as tf:
    for m in tf:
        b = os.path.basename(m.name)
        if not b.endswith(".wav") or "_acoustic_" not in b:
            continue
        x = sf.read(io.BytesIO(tf.extractfile(m).read()), dtype="int16")[0].astype(np.int64)
        x = x[: (len(x) // 1024) * 1024]
        xc = ev.chunks(x)
        d = ev.patched_bits(ev.fixed_residual(xc, 1)[:, 1:]) / x.size
        dd = (ev.patched_bits(ev.fixed_residual(xc, 2)[:, 2:]) + ev.warmup_bits(xc, 2).sum()) / x.size
        best = 1e9
        for p in (4, 8, 16, 32):
            r, h = ev.lpc_chunk_residual(xc, p)
            best = min(best, (ev.patched_bits(r) + h.sum()) / x.size)
        rows.append((min(d, dd) / best, b, min(d, dd), best))
rows.sort(reverse=True)
g = np.array([r[0] for r in rows])
print(
    "notes",
    len(g),
    f"median {np.median(g):.2f}",
    f"p90 {np.percentile(g, 90):.2f}",
    ">=1.5x:",
    (g >= 1.5).sum(),
    ">=2x:",
    (g >= 2).sum(),
)
for r in rows[:12]:
    print(f"{r[0]:.2f}x {r[1]} fixed {r[2]:.2f} lpc {r[3]:.2f}")


band = collections.defaultdict(list)
for gain, b, fixed, lpc in rows:
    pitch = int(b.rsplit("-", 2)[1])
    band[(pitch // 12) * 12].append((gain, fixed, lpc))
print("| MIDI pitch | fundamental | samples/cycle at 16 kHz | notes | median gain | total-size gain | notes >=2x |")
print("|---|---|---|---|---|---|---|")
for lo in sorted(band):
    v = band[lo]
    f = 440 * 2 ** ((lo + 6 - 69) / 12)
    tot = sum(x[1] for x in v) / sum(x[2] for x in v)
    print(
        f"| {lo}-{lo + 11} | {f:.0f} Hz | {16000 / f:.0f} | {len(v)} | {np.median([x[0] for x in v]):.2f}x | {tot:.2f}x | {sum(x[0] >= 2 for x in v)} |"
    )
