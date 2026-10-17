# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Summarize leaf-lab results: sizes vs the cascade, compression ratios and timings.

Usage: python3 analyze.py <results.jsonl> [sizes|ratios|timing|leaves|all]
"""

import collections
import json
import sys

CODERS = [
    ("cascade (today)", "cascade"),
    ("BitPacked", "bitpacked"),
    ("Rice", "rice"),
    ("Exp-Golomb", "expgolomb"),
    ("Huffman x16", "bins+huff12x16"),
    ("Huffman x32", "bins+huff12x32"),
    ("Tunstall-12", "bins+tunstall12"),
    ("Tunstall-16", "bins+tunstall16"),
    ("rANS x16", "bins+rans12x16"),
    ("rANS x32", "bins+rans12x32"),
    ("tANS eb-4k lag0", "eb-4k"),
    ("eb lag0 planned block", "eb-lag0"),
    ("static H0 bound", "bins+H0"),
    ("adaptive o0", "bins+adaptive-o0"),
    ("adaptive o1", "bins+adaptive-o1"),
    ("eb with lag", "eb"),
]


def category(path):
    p = path.lstrip("/")
    head = p.split("vortex.dict.codes")[0]
    if "alp.encoded" in p:
        return "ALP-encoded ints"
    if "dict.codes" in p and not any(x in head for x in ("runend", "rle", "sparse")):
        return "dict codes"
    if p.startswith("vortex.runend.ends"):
        return "RunEnd ends (delta)"
    if p.startswith("vortex.runend.values"):
        return "RunEnd values"
    if p.startswith("fastlanes.for"):
        return "FoR residuals"
    if p.startswith("vortex.dict.values/vortex.fsst"):
        return "FSST lengths/offsets"
    if p.startswith("vortex.dict.values"):
        return "int dict values"
    if "rle" in p:
        return "RLE indices/values"
    if "sparse" in p:
        return "Sparse patches"
    if "datetimeparts" in p:
        return "DateTimeParts seconds"
    if p.startswith("fastlanes.bitpacked"):
        return "plain BitPacked"
    return "other"


def size(r, c):
    v = r["coders"].get(c)
    return v["bytes"] if v else r["coders"]["cascade"]["bytes"]


def raw(rs):
    return sum(r["n"] * r["width"] for r in rs)


def throughput(rs, c, key):
    rs = [r for r in rs if r["coders"].get(c, {}).get(key)]
    ns = sum(r["coders"][c][key] for r in rs)
    if not ns:
        return None
    n = sum(r["n"] for r in rs)
    return n / ns * 1e3, raw(rs) / ns


def sizes_and_ratios(R):
    groups = {
        "all": R,
        "64-bit": [r for r in R if r["width"] == 8],
        "narrow": [r for r in R if r["width"] < 8],
    }
    n = sum(r["n"] for r in R)
    print(f"{len(R)} leaves, {n / 1e6:.1f}M values")
    print(f"{'coder':24s}" + "".join(f"{k:>20s}" for k in groups) + f"{'MB':>8s}{'bits/v':>8s}")
    for name, c in CODERS:
        cells = []
        for rs in groups.values():
            b = sum(size(r, c) for r in rs)
            casc = sum(size(r, "cascade") for r in rs)
            cells.append(f"{raw(rs) / b:6.2f}x ({b / casc * 100:5.1f}%)")
        b = sum(size(r, c) for r in R)
        print(f"{name:24s}" + "".join(f"{x:>20s}" for x in cells) + f"{b / 1e6:8.2f}{b * 8 / n:8.2f}")

    sub = [r for r in R if "sym+huff15x16" in r["coders"]]
    print(f"\nsmall-alphabet leaves ({len(sub)}): ratio with bins / direct symbols")
    for name, b, d in [
        ("Huffman", "bins+huff12x16", "sym+huff15x16"),
        ("rANS", "bins+rans12x16", "sym+rans15x16"),
        ("Tunstall-16", "bins+tunstall16", "sym+tunstall16"),
        ("adaptive o0", "bins+adaptive-o0", "sym+adaptive-o0"),
        ("H0 bound", "bins+H0", "sym+H0"),
    ]:
        print(f"  {name:12s} {raw(sub) / sum(size(r, b) for r in sub):6.2f}x  {raw(sub) / sum(size(r, d) for r in sub):6.2f}x")

    cats = collections.defaultdict(list)
    for r in R:
        cats[category(r["path"])].append(r)
    cols = [("casc", "cascade"), ("Rice", "rice"), ("Huff", "bins+huff12x16"), ("Tun16", "bins+tunstall16"),
            ("rANS", "bins+rans12x16"), ("tANS", "eb-4k"), ("H0", "bins+H0"), ("ad-o0", "bins+adaptive-o0"),
            ("ad-o1", "bins+adaptive-o1"), ("eb+lag", "eb")]
    print(f"\n{'category ratio':24s}{'casc MB':>8s}" + "".join(f"{a:>8s}" for a, _ in cols))
    for k, rs in sorted(cats.items(), key=lambda x: -sum(size(r, "cascade") for r in x[1])):
        casc = sum(size(r, "cascade") for r in rs)
        print(f"{k:24s}{casc / 1e6:8.2f}" + "".join(f"{raw(rs) / sum(size(r, c) for r in rs):8.2f}" for _, c in cols))


def timing(R):
    n = sum(r["n"] for r in R)
    bins_ns = sum(r["coders"]["bins_train_ns"] for r in R)
    print(f"bins train+split: {n / bins_ns * 1e3:.0f} Mv/s")
    print(f"{'coder':24s}{'enc Mv/s':>10s}{'+bins':>8s}{'dec Mv/s':>10s}{'dec GB/s':>10s}{'ids Mv/s':>10s}")
    for name, c in CODERS:
        d = throughput(R, c, "ns")
        if not d:
            continue
        e = throughput(R, c, "enc_ns")
        i = throughput(R, c, "ids_ns")
        with_bins = ""
        if c.startswith("bins+"):
            enc = sum(r["coders"][c]["enc_ns"] for r in R)
            with_bins = f"{n / (enc + bins_ns) * 1e3:.0f}"
        print(f"{name:24s}{e[0]:10.0f}{with_bins:>8s}{d[0]:10.0f}{d[1]:10.2f}{(f'{i[0]:.0f}' if i else ''):>10s}")


def leaves(R):
    agg = collections.defaultdict(lambda: collections.Counter())
    for r in R:
        a = agg[(r["src"].split(".", 1)[1], r["path"].lstrip("/"))]
        for c in ("cascade", "eb-4k", "bins+adaptive-o1", "eb"):
            a[c] += size(r, c)
    print("top leaves by bytes saved (eb-4k vs cascade)")
    for (col, path), a in sorted(agg.items(), key=lambda kv: kv[1]["eb-4k"] - kv[1]["cascade"])[:25]:
        print(f"  {col:20s} {path:62s} {a['cascade'] / 1e3:7.0f}K  eb-4k {a['eb-4k'] / a['cascade'] * 100:5.1f}%"
              f"  ad-o1 {a['bins+adaptive-o1'] / a['cascade'] * 100:5.1f}%  eb+lag {a['eb'] / a['cascade'] * 100:5.1f}%")


def main():
    R = [json.loads(line) for line in open(sys.argv[1])]
    what = sys.argv[2] if len(sys.argv) > 2 else "all"
    if what in ("sizes", "ratios", "all"):
        sizes_and_ratios(R)
    if what in ("timing", "all"):
        print()
        timing(R)
    if what in ("leaves", "all"):
        print()
        leaves(R)


if __name__ == "__main__":
    main()
