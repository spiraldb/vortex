// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Length-limited canonical Huffman with `S` interleaved bit streams per block (value i goes to
//! stream i % S), table-driven decode.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::SymbolCoder;
use crate::bits::BitReader;
use crate::bits::BitWriter;
use crate::bits::bits;

pub struct Huffman {
    pub streams: usize,
    pub max_len: u32,
    lens: Vec<u8>,
    /// Bit-reversed canonical code per symbol, for LSB-first writing.
    codes: Vec<u32>,
    /// `1 << max_len` entries of (symbol, length).
    table: Vec<(u16, u8)>,
}

impl Huffman {
    pub fn new(streams: usize, max_len: u32) -> Self {
        Self { streams, max_len, lens: vec![], codes: vec![], table: vec![] }
    }
}

/// Huffman code lengths, limited to `max_len` by demoting the deepest-possible symbols until the
/// Kraft sum fits.
pub fn code_lengths(counts: &[u32], max_len: u32) -> Vec<u8> {
    let n = counts.len();
    let mut lens = vec![0u8; n];
    let live: Vec<usize> = (0..n).filter(|&i| counts[i] > 0).collect();
    if live.len() == 1 {
        lens[live[0]] = 1;
        return lens;
    }
    // Node arena: leaves then internal nodes.
    let mut parent = vec![usize::MAX; 2 * n];
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> =
        live.iter().map(|&i| Reverse((u64::from(counts[i]), i))).collect();
    let mut next = n;
    while heap.len() > 1 {
        let Reverse((a, ia)) = heap.pop().unwrap();
        let Reverse((b, ib)) = heap.pop().unwrap();
        parent[ia] = next;
        parent[ib] = next;
        heap.push(Reverse((a + b, next)));
        next += 1;
    }
    for &i in &live {
        let mut d = 0;
        let mut p = parent[i];
        while p != usize::MAX {
            d += 1;
            p = parent[p];
        }
        lens[i] = d.min(max_len) as u8;
    }
    // Restore Kraft: lengthen the codes that cost least (lowest counts with room to grow).
    let full = 1u64 << max_len;
    let mut kraft: u64 = live.iter().map(|&i| full >> lens[i]).sum();
    let mut order = live.clone();
    order.sort_by_key(|&i| counts[i]);
    while kraft > full {
        for &i in &order {
            if u32::from(lens[i]) < max_len {
                kraft -= full >> (lens[i] + 1);
                lens[i] += 1;
                if kraft <= full {
                    break;
                }
            }
        }
    }
    lens
}

impl SymbolCoder for Huffman {
    fn name(&self) -> String {
        format!("huff{}x{}", self.max_len, self.streams)
    }

    fn prepare(&mut self, counts: &[u32]) -> usize {
        self.lens = code_lengths(counts, self.max_len);
        let mut codes = vec![0u32; counts.len()];
        let mut sorted: Vec<usize> = (0..counts.len()).filter(|&i| self.lens[i] > 0).collect();
        sorted.sort_by_key(|&i| (self.lens[i], i));
        let mut code = 0u32;
        let mut prev_len = 0u8;
        self.table = vec![(0, 0); 1 << self.max_len];
        for &s in &sorted {
            let l = self.lens[s];
            code <<= l - prev_len;
            prev_len = l;
            let rev = code.reverse_bits() >> (32 - u32::from(l));
            codes[s] = rev;
            let step = 1usize << l;
            let mut j = rev as usize;
            while j < self.table.len() {
                self.table[j] = (s as u16, l);
                j += step;
            }
            code += 1;
        }
        self.codes = codes;
        // Code lengths: 4 bits per symbol of the alphabet (5 when lengths exceed 15).
        let per = if self.max_len > 15 { 5 } else { 4 };
        (counts.len() * per).div_ceil(8)
    }

    fn encode(&self, syms: &[u16]) -> Vec<u8> {
        let s = self.streams;
        let mut writers: Vec<BitWriter> = (0..s).map(|_| BitWriter::new()).collect();
        for (i, &sym) in syms.iter().enumerate() {
            writers[i % s].write(u64::from(self.codes[sym as usize]), u32::from(self.lens[sym as usize]));
        }
        let streams: Vec<Vec<u8>> = writers.into_iter().map(|w| w.finish()).collect();
        // Header: width of the stream lengths, then S-1 lengths (the last is implied).
        let width = streams.iter().map(|b| bits(b.len() as u64)).max().unwrap_or(0);
        let mut header = BitWriter::new();
        header.write(u64::from(width), 5);
        for b in &streams[..s - 1] {
            header.write(b.len() as u64, width);
        }
        let mut out = header.finish();
        for b in streams {
            out.extend_from_slice(&b);
        }
        out
    }

    fn decode(&self, bytes: &[u8], out: &mut [u16]) {
        let s = self.streams;
        let mut header = BitReader::new(bytes);
        let width = header.read(5) as u32;
        let mut starts = vec![0usize; s];
        let mut pos = 0;
        for i in 0..s - 1 {
            starts[i] = pos;
            pos += header.read(width) as usize;
        }
        starts[s - 1] = pos;
        let base = header.bit_pos().div_ceil(8);
        let mut readers: Vec<BitReader> =
            starts.iter().map(|&st| BitReader::new(&bytes[base + st..])).collect();
        let mask = (1u64 << self.max_len) - 1;
        let n = out.len();
        let full = n / s * s;
        let mut i = 0;
        while i < full {
            for (lane, r) in readers.iter_mut().enumerate() {
                let (sym, len) = self.table[(r.peek() & mask) as usize];
                r.skip(u32::from(len));
                out[i + lane] = sym;
            }
            i += s;
        }
        for (lane, r) in readers.iter_mut().enumerate().take(n - full) {
            let (sym, len) = self.table[(r.peek() & mask) as usize];
            r.skip(u32::from(len));
            out[full + lane] = sym;
        }
    }
}
