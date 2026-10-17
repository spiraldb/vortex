# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""BitPacked vs the other leaf coders and pco: ratio, compression and decompression speed.

Usage: python3 versus.py <results.jsonl>
"""

import collections
import json
import sys

from analyze import category

R = [json.loads(line) for line in open(sys.argv[1])]
R = [r for r in R if "bitpacked" in r["coders"]]
CODERS = [
    ("BitPacked", "bitpacked"),
    ("tANS (eb-4k, lag 0)", "eb-4k"),
    ("rANS x16", "bins+rans12x16"),
    ("Huffman x16", "bins+huff12x16"),
    ("Tunstall-12", "bins+tunstall12"),
    ("adaptive o1", "bins+adaptive-o1"),
    ("eb with lag", "eb"),
    ("pco leaf (Classic, no delta)", "pco-leaf"),
    ("pco default", "pco"),
]


def stats(rs, c):
    raw = sum(r["n"] * r["width"] for r in rs)
    n = sum(r["n"] for r in rs)
    b = sum(r["coders"][c]["bytes"] for r in rs)
    enc = sum(r["coders"][c]["enc_ns"] for r in rs)
    if c.startswith("bins+"):
        enc += sum(r["coders"]["bins_train_ns"] for r in rs)
    dec = sum(r["coders"][c]["ns"] for r in rs)
    return raw / b, n / enc * 1e3, raw / enc, n / dec * 1e3, raw / dec


def table(rs, title):
    print(f"\n{title}: {len(rs)} leaves, {sum(r['n'] for r in rs) / 1e6:.1f}M values")
    print(f"{'coder':30s}{'ratio':>7s}{'vs BP':>7s}{'enc Mv/s':>10s}{'enc GB/s':>9s}{'dec Mv/s':>10s}{'dec GB/s':>9s}")
    bp = stats(rs, "bitpacked")[0]
    for name, c in CODERS:
        ratio, em, eg, dm, dg = stats(rs, c)
        print(f"{name:30s}{ratio:7.2f}{ratio / bp:6.2f}x{em:10.0f}{eg:9.2f}{dm:10.0f}{dg:9.2f}")


table(R, "all")
table([r for r in R if r["width"] == 8], "64-bit")
table([r for r in R if r["width"] < 8], "narrow")

cats = collections.defaultdict(list)
for r in R:
    cats[category(r["path"])].append(r)
print("\nper category: ratio / decode Mv/s")
cols = ["bitpacked", "eb-4k", "bins+adaptive-o1", "pco-leaf", "pco"]
print(f"{'category':24s}" + "".join(f"{c:>20s}" for c in cols))
for k, rs in sorted(cats.items(), key=lambda x: -sum(r["coders"]["bitpacked"]["bytes"] for r in x[1])):
    cells = []
    for c in cols:
        s = stats(rs, c)
        cells.append(f"{s[0]:6.2f}x {s[3]:6.0f}")
    print(f"{k:24s}" + "".join(f"{x:>20s}" for x in cells))

# Per leaf: how often does each coder beat BitPacked by >= 10%?
print("\nleaves where coder is >= 10% smaller than BitPacked (share of BitPacked bytes in those leaves)")
tot = sum(r["coders"]["bitpacked"]["bytes"] for r in R)
for name, c in CODERS[1:]:
    win = [r for r in R if r["coders"][c]["bytes"] <= 0.9 * r["coders"]["bitpacked"]["bytes"]]
    print(f"  {name:30s} {len(win):4d} leaves, {sum(r['coders']['bitpacked']['bytes'] for r in win) / tot * 100:5.1f}% of bytes")
