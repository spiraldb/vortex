# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Whole-column comparison from the `columns` binary, focused on columns where pco does well.

Usage: python3 columns.py <columns.jsonl> [pco threshold, default 0.9]
"""

import collections
import json
import sys

R = [json.loads(line) for line in open(sys.argv[1])]
THRESH = float(sys.argv[2]) if len(sys.argv) > 2 else 0.9
SETUPS = ["btr", "btr+eb", "btr+eb-smallest", "compact", "compact+eb", "pco"]


def agg(rs, s):
    rs = [r for r in rs if s in r]
    raw = sum(r["raw"] for r in rs)
    n = sum(r["n"] for r in rs)
    b = sum(r[s]["bytes"] for r in rs)
    enc = sum(r[s]["enc_ns"] for r in rs)
    dec = sum(r[s]["dec_ns"] for r in rs)
    encs = collections.Counter()
    for r in rs:
        encs.update(r[s]["encodings"])
    return raw / b, b, n / enc * 1e3, n / dec * 1e3, raw / dec, encs


def table(rs, title):
    print(f"\n{title}: {len(rs)} column slices, {sum(r['n'] for r in rs) / 1e6:.1f}M values")
    print(f"{'setup':18s}{'ratio':>8s}{'MB':>8s}{'vs btr':>8s}{'enc Mv/s':>10s}{'dec Mv/s':>10s}{'dec GB/s':>10s}  eb/pco share")
    base = agg(rs, "btr")[1]
    for s in SETUPS:
        ratio, b, em, dm, dg, encs = agg(rs, s)
        tot = sum(encs.values())
        eb = sum(v for k, v in encs.items() if "entropy" in k) / tot * 100
        pc = sum(v for k, v in encs.items() if "pco" in k) / tot * 100
        print(f"{s:18s}{ratio:8.2f}{b / 1e6:8.2f}{b / base * 100:7.1f}%{em:10.0f}{dm:10.0f}{dg:10.2f}  eb {eb:4.0f}% pco {pc:4.0f}%")


have_pco = [r for r in R if "pco" in r]
good = [r for r in have_pco if r["pco"]["bytes"] <= THRESH * r["btr"]["bytes"]]
table(R, "all numeric columns")
table(good, f"columns where pco <= {THRESH:.0%} of btr")

cols = collections.defaultdict(list)
for r in good:
    cols[r["col"]].append(r)
print(f"\nper column (both batches): size as % of btr; ratio of btr in brackets")
print(f"{'column':28s}{'ptype':>6s}{'btr':>14s}" + "".join(f"{s:>16s}" for s in SETUPS[1:]) + "   btr+eb tree")
for col, rs in sorted(cols.items(), key=lambda kv: -sum(r["btr"]["bytes"] for r in kv[1])):
    btr = sum(r["btr"]["bytes"] for r in rs)
    raw = sum(r["raw"] for r in rs)
    cells = "".join(f"{sum(r[s]['bytes'] for r in rs) / btr * 100:15.1f}%" for s in SETUPS[1:])
    encs = collections.Counter()
    for r in rs:
        encs.update(r["btr+eb"]["encodings"])
    top = ",".join(k.split(".")[-1] for k, v in encs.most_common(3) if v > 0)
    print(f"{col:28s}{rs[0]['ptype']:>6s}{btr / 1e3:7.0f}K ({raw / btr:4.1f}x)" + cells + f"   {top}")
