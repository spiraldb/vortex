// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Tunstall: variable-to-fixed codes. The dictionary is a complete parse tree grown by expanding
//! the most probable leaf while the leaf count fits `1 << code_bits`. Every codeword has the same
//! width, so codeword k of a block starts at bit k * code_bits; only output positions need a
//! prefix sum, which is what makes the decode trivially parallel.

use std::collections::BinaryHeap;

use crate::SymbolCoder;
use crate::bits::BitReader;
use crate::bits::BitWriter;
use crate::freq_table_bytes;
use crate::normalize;

pub struct Tunstall {
    pub code_bits: u32,
    /// Live symbols (nonzero counts) and their index in the alphabet.
    live: Vec<u16>,
    live_of: Vec<u16>,
    /// Per tree node: first child index (children are contiguous, one per live symbol), or 0 for
    /// a leaf; and the leaf's code.
    child: Vec<u32>,
    code: Vec<u32>,
    /// Per code: (start, len) into `pool`.
    entries: Vec<(u32, u32)>,
    pool: Vec<u16>,
}

impl Tunstall {
    pub fn new(code_bits: u32) -> Self {
        Self {
            code_bits,
            live: vec![],
            live_of: vec![],
            child: vec![],
            code: vec![],
            entries: vec![],
            pool: vec![],
        }
    }
}

#[derive(PartialEq, PartialOrd)]
struct P(f64, u32);
impl Eq for P {}
impl Ord for P {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.partial_cmp(o).unwrap()
    }
}

impl SymbolCoder for Tunstall {
    fn name(&self) -> String {
        format!("tunstall{}", self.code_bits)
    }

    fn prepare(&mut self, counts: &[u32]) -> usize {
        // The decoder rebuilds the tree from the same normalized weights the table stores.
        let norm = normalize(counts, 15);
        self.live = (0..counts.len() as u16).filter(|&s| norm[s as usize] > 0).collect();
        self.live_of = vec![u16::MAX; counts.len()];
        for (k, &s) in self.live.iter().enumerate() {
            self.live_of[s as usize] = k as u16;
        }
        let a = self.live.len();
        let total: f64 = norm.iter().map(|&f| f64::from(f)).sum();
        let p: Vec<f64> = self.live.iter().map(|&s| f64::from(norm[s as usize]) / total).collect();
        let max_leaves = 1usize << self.code_bits;
        // Node 0 is the root (expanded); its children are nodes 1..=a.
        self.child = vec![1];
        let mut prob = vec![1.0];
        let mut parent = vec![u32::MAX];
        let mut sym = vec![u16::MAX];
        let mut heap = BinaryHeap::new();
        for k in 0..a {
            self.child.push(0);
            prob.push(p[k]);
            parent.push(0);
            sym.push(k as u16);
            heap.push(P(p[k], (k + 1) as u32));
        }
        let mut leaves = a;
        if a > 1 {
            while leaves + a - 1 <= max_leaves {
                let P(pr, node) = heap.pop().unwrap();
                let first = self.child.len() as u32;
                self.child[node as usize] = first;
                for k in 0..a {
                    self.child.push(0);
                    prob.push(pr * p[k]);
                    parent.push(node);
                    sym.push(k as u16);
                    heap.push(P(pr * p[k], first + k as u32));
                }
                leaves += a - 1;
            }
        }
        // Assign codes to leaves and spell out each leaf's string.
        self.code = vec![u32::MAX; self.child.len()];
        self.entries.clear();
        self.pool.clear();
        for node in 1..self.child.len() {
            if self.child[node] != 0 {
                continue;
            }
            self.code[node] = self.entries.len() as u32;
            let mut s = Vec::new();
            let mut n = node as u32;
            while n != 0 {
                s.push(self.live[sym[n as usize] as usize]);
                n = parent[n as usize];
            }
            s.reverse();
            self.entries.push((self.pool.len() as u32, s.len() as u32));
            self.pool.extend_from_slice(&s);
        }
        freq_table_bytes(&norm)
    }

    fn encode(&self, syms: &[u16]) -> Vec<u8> {
        if self.live.len() == 1 {
            return Vec::new();
        }
        let mut w = BitWriter::new();
        let mut cur = 0usize;
        for &s in syms {
            cur = (self.child[cur] + u32::from(self.live_of[s as usize])) as usize;
            if self.child[cur] == 0 {
                w.write(u64::from(self.code[cur]), self.code_bits);
                cur = 0;
            }
        }
        // Finish a partial parse with any leaf below it; the decoder truncates at the block length.
        if cur != 0 {
            while self.child[cur] != 0 {
                cur = self.child[cur] as usize;
            }
            w.write(u64::from(self.code[cur]), self.code_bits);
        }
        w.finish()
    }

    fn decode(&self, bytes: &[u8], out: &mut [u16]) {
        if self.live.len() == 1 {
            out.fill(self.live[0]);
            return;
        }
        let mut r = BitReader::new(bytes);
        let n = out.len();
        let mut i = 0;
        while i < n {
            let (start, len) = self.entries[r.read(self.code_bits) as usize];
            let len = (len as usize).min(n - i);
            out[i..i + len].copy_from_slice(&self.pool[start as usize..start as usize + len]);
            i += len;
        }
    }
}
