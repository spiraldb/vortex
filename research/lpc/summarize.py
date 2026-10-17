# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Print Markdown tables from results.jsonl: per-series bits/value and per-category LPC gains."""

import collections
import json
import math
import sys

rows = [json.loads(line) for line in open(sys.argv[1])]
rows = {r["name"]: r for r in rows}.values()
LIGHT = ["for", "delta", "dod", "affine"]


def best(r, keys):
    k = min(keys, key=lambda k: r[k])
    return k, r[k]


out = []
for r in rows:
    coder = "patch"
    bk, bl = best(r, [f"{k}:{coder}" for k in LIGHT])
    lk, lv = best(
        r, [f"lpc{p}/lanes:{coder}" for p in (4, 8, 16, 32)] + [f"lpc{p}/chunk:{coder}" for p in (2, 4, 8, 16, 32)]
    )
    _, any_lpc = best(r, [f"lpc{p}/block:{coder}" for p in (2, 4, 8, 16, 32)])
    ak, av = best(r, [f"{k}:ans" for k in LIGHT])
    lak, lav = best(r, [f"lpc{p}/lanes:ans" for p in (4, 8, 16, 32)] + [f"lpc{p}/chunk:ans" for p in (2, 4, 8, 16, 32)])
    out.append(
        dict(
            name=r["name"],
            cat=r["category"],
            rate=r["rate"],
            n=r["n_eval"],
            raw=r["for:patch"],
            base=bk.split(":")[0],
            base_bits=bl,
            lpc=lk.split(":")[0],
            lpc_bits=lv,
            gain=bl / lv,
            any_gain=bl / any_lpc,
            ans_base=av,
            ans_lpc=lav,
            ans_gain=av / lav,
            pco_auto=r["pco-auto"],
            pco_conv1=r["pco-conv1"],
            conv1_gain=r["pco-consec"] / r["pco-conv1"] if r["pco-conv1"] == r["pco-conv1"] else float("nan"),
            lpc_vs_pco=r["pco-auto"] / lv,
            zstd19=r["zstd19"],
        )
    )
out.sort(key=lambda o: -o["gain"])
print(
    "| series | category | rate Hz | best non-LPC (bits) | LPC, lane-parallel (bits) | LPC gain | non-LPC+ANS | LPC+ANS | ANS gain | pco auto | pco Conv1 | Conv1 gain | zstd-19 |"
)
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for o in out:
    rate = "" if not o["rate"] else (f"{o['rate']:g}" if o["rate"] >= 1 else f"1/{1 / o['rate']:.0f}")
    print(
        f"| {o['name']} | {o['cat']} | {rate} | {o['base']} {o['base_bits']:.2f} | {o['lpc']} {o['lpc_bits']:.2f} | **{o['gain']:.2f}x** | {o['ans_base']:.2f} | {o['ans_lpc']:.2f} | {o['ans_gain']:.2f}x | {o['pco_auto']:.2f} | {o['pco_conv1']:.2f} | {o['conv1_gain']:.2f}x | {o['zstd19']:.2f} |"
    )
print()
cats = collections.defaultdict(list)
for o in out:
    cats[o["cat"]].append(o)


def g(xs):
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


print(
    "| category | series | geo-mean LPC gain (bit-packed) | min..max | geo-mean LPC gain (ANS residuals) | LPC beats pco auto in |"
)
print("|---|---|---|---|---|---|")
for c, os_ in sorted(cats.items(), key=lambda kv: -g([o["gain"] for o in kv[1]])):
    gs = [o["gain"] for o in os_]
    print(
        f"| {c} | {len(os_)} | {g(gs):.2f}x | {min(gs):.2f}..{max(gs):.2f} | {g([o['ans_gain'] for o in os_]):.2f}x | {sum(o['lpc_vs_pco'] > 1 for o in os_)}/{len(os_)} |"
    )
print()
print("orders chosen:", collections.Counter(o["lpc"] for o in out))
print(
    "n series",
    len(out),
    "gain>=1.10:",
    sum(o["gain"] >= 1.10 for o in out),
    "gain>=1.05:",
    sum(o["gain"] >= 1.05 for o in out),
)
