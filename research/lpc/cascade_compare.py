# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare Delta, delta-of-delta and LPC when each residual stream gets the same real Vortex cascade.

Each side is the best of the BtrBlocks configurations run by `affine_eval` on its residual column,
plus its own header bits: chunk first values for Delta, plus warm-ups for delta-of-delta, and
coefficients plus warm-ups for LPC.

Usage: python -I cascade_compare.py <export_dir> <data_dir> <affine_eval TSV>...
"""

import collections
import json
import math
import os
import sys

out_dir, data, *tsvs = sys.argv[1:]
cats = {}
for line in open(os.path.join(data, "manifest.jsonl")):
    r = json.loads(line)
    cats[r["name"]] = r["category"]

sizes = collections.defaultdict(dict)
trees = {}
for tsv in tsvs:
    for line in open(tsv).read().splitlines()[1:]:
        col, rows, config, bits, tree = line.split("\t")[:5]
        sizes[col][config] = float(bits) * int(rows)
        trees[(col, config)] = tree


def best(col):
    cfg = min(sizes[col], key=sizes[col].get)
    return sizes[col][cfg], trees[(col, cfg)]


lpc_hdr = {}
for line in open(os.path.join(out_dir, "lpc_headers.tsv")):
    name, n, order, header = line.split("\t")
    lpc_hdr[name] = (int(n), order.split(":")[0], float(header))

rows = []
for line in open(os.path.join(out_dir, "fixed_headers.tsv")):
    name, n, delta_hdr, dod_hdr = line.split("\t")
    n = int(n)
    d_bits, d_tree = best(name + "__delta")
    dd_bits, _ = best(name + "__dod")
    l_bits, l_tree = best(name + "__lpc")
    o_bits, o_tree = best(name)
    delta = (d_bits + float(delta_hdr)) / n
    dod = (dd_bits + float(dod_hdr)) / n
    lpc = (l_bits + lpc_hdr[name][2]) / n
    rows.append(
        dict(
            name=name,
            cat=cats.get(name, "?"),
            orig=o_bits / n,
            delta=delta,
            dod=dod,
            lpc=lpc,
            order=lpc_hdr[name][1],
            d_tree=d_tree,
            l_tree=l_tree,
            vs_delta=delta / lpc,
            vs_fixed=min(delta, dod) / lpc,
        )
    )
rows.sort(key=lambda r: -r["vs_delta"])


def gm(xs):
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


by = collections.defaultdict(list)
for r in rows:
    by[r["cat"]].append(r)
print("| category | series | LPC vs Delta | min..max | LPC vs best of Delta, DoD | LPC smaller than both in |")
print("|---|---|---|---|---|---|")
for c, rs in sorted(by.items(), key=lambda kv: -gm([r["vs_delta"] for r in kv[1]])):
    g = [r["vs_delta"] for r in rs]
    f = [r["vs_fixed"] for r in rs]
    print(
        f"| {c} | {len(rs)} | {gm(g):.2f}x | {min(g):.2f}..{max(g):.2f} | {gm(f):.2f}x | {sum(x > 1 for x in f)}/{len(f)} |"
    )
print()
print(
    f"series {len(rows)}: LPC >=5% smaller than cascaded Delta in {sum(r['vs_delta'] >= 1.05 for r in rows)}, "
    f">=10% in {sum(r['vs_delta'] >= 1.10 for r in rows)}; than the better of Delta/DoD: "
    f">=5% in {sum(r['vs_fixed'] >= 1.05 for r in rows)}, >=10% in {sum(r['vs_fixed'] >= 1.10 for r in rows)}"
)
print()
print(
    "| series | category | original, best Vortex | Delta + cascade (tree) | DoD + cascade | LPC + cascade (tree) | vs Delta | vs best fixed |"
)
print("|---|---|---|---|---|---|---|---|")
for r in rows:
    print(
        f"| {r['name']} | {r['cat']} | {r['orig']:.2f} | {r['delta']:.2f} `{r['d_tree']}` | {r['dod']:.2f} | "
        f"{r['lpc']:.2f} {r['order']} `{r['l_tree']}` | **{r['vs_delta']:.2f}x** | {r['vs_fixed']:.2f}x |"
    )
