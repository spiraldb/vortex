# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare real Vortex sizes of each column against its LPC residual column plus LPC headers.

Usage: python -I vortex_compare.py <export_dir with lpc_headers.tsv> <affine_eval output TSV> <data_dir>
"""

import collections
import json
import math
import os
import sys

out_dir, tsv, data = sys.argv[1:4]
cats = {}
for line in open(os.path.join(data, "manifest.jsonl")):
    r = json.loads(line)
    cats[r["name"]] = r["category"]

sizes = collections.defaultdict(dict)
trees = {}
for line in open(tsv).read().splitlines()[1:]:
    col, rows, config, bits, tree = line.split("\t")[:5]
    sizes[col][config] = float(bits) * int(rows)
    trees[(col, config)] = tree

rows = []
for line in open(os.path.join(out_dir, "lpc_headers.tsv")):
    name, n, best, header = line.split("\t")
    n, header = int(n), float(header)
    orig = sizes[name]
    res = sizes[name + "__lpc"]
    default = orig["default"] / n
    base_cfg = min(orig, key=orig.get)
    base = orig[base_cfg] / n
    res_cfg = min(res, key=res.get)
    lpc = (res[res_cfg] + header) / n
    rows.append(
        dict(
            name=name,
            cat=cats.get(name, "?"),
            default=default,
            base=base,
            base_tree=trees[(name, base_cfg)],
            lpc=lpc,
            lpc_tree=trees[(name + "__lpc", res_cfg)],
            order=best.split(":")[0],
            gain=base / lpc,
        )
    )
rows.sort(key=lambda r: -r["gain"])

print("| series | category | Vortex default | best Vortex (tree) | LPC + Vortex on residuals (residual tree) | gain |")
print("|---|---|---|---|---|---|")
for r in rows:
    print(
        f"| {r['name']} | {r['cat']} | {r['default']:.2f} | {r['base']:.2f} `{r['base_tree']}` | "
        f"{r['lpc']:.2f} {r['order']} `{r['lpc_tree']}` | **{r['gain']:.2f}x** |"
    )
print()


def gm(xs):
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


by = collections.defaultdict(list)
for r in rows:
    by[r["cat"]].append(r)
print("| category | series | geo-mean gain over best Vortex | min..max | LPC smaller in |")
print("|---|---|---|---|---|")
for c, rs in sorted(by.items(), key=lambda kv: -gm([r["gain"] for r in kv[1]])):
    g = [r["gain"] for r in rs]
    print(f"| {c} | {len(rs)} | {gm(g):.2f}x | {min(g):.2f}..{max(g):.2f} | {sum(x > 1 for x in g)}/{len(g)} |")
print()
print(
    f"series: {len(rows)}, LPC >=5% smaller: {sum(r['gain'] >= 1.05 for r in rows)}, >=10%: {sum(r['gain'] >= 1.10 for r in rows)}"
)
