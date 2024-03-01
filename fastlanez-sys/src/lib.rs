/*
 * (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#![feature(generic_const_exprs)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::mem::size_of;
use seq_macro::seq;
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

struct Pred<const B: bool>;
trait Satisfied {}
impl Satisfied for Pred<true> {}

trait FLUnsigned {}

impl FLUnsigned for u8 {}
impl FLUnsigned for u16 {}
impl FLUnsigned for u32 {}
impl FLUnsigned for u64 {}

// TODO(ngates): export packed widths in the C API so we can check for support here.
trait BitPack<T: FLUnsigned, const W: usize> where Pred<{ W > 0 }>: Satisfied, Pred<{ W < size_of::<T>() * 8 }>: Satisfied {
    fn pack(self: Self, output: &mut [T; 128 * W]);
}

macro_rules! impl_bitpack {
    ($t:ty) => {
        paste::item! {
            seq!(W in 1..size_of!($t) {
                impl<'a> BitPack<$t, W> for &'a[$t; 1024]  {
                    fn pack(self: &'a[$t; 1024], output: &mut [$t; 128 * W])  {
                        unsafe { [<fl_bitpack_ $t _u>]~W(self, output) }
                    }
                }
            });
            //
            // fn [<bitpack_ $t>](width: u8, input: &[T; 1024]) -> Vec<u8> {
            //     match (width) {
            //         seq!(W in 1..size_of::<$t>() * 8 {
            //            W => {
            //                 let mut output = [0u8; 128 * W];
            //                 BitPack::<$t, W>::pack(input, &mut output);
            //                 output.to_vec()
            //            }
            //         });
            //         w @ 1  ..= 12 => {
            //             let mut output = [0u8; 128 * w as usize];
            //             BitPack::<T, w>::pack(input, &mut output);
            //             output.to_vec()
            //         },
            //     }
            // }
        }
    }
}

impl_bitpack!(u8);
// impl_bitpack!(u16);
// impl_bitpack!(u32);
// impl_bitpack!(u64);

// fn bitpack<T: FLUnsigned>(width: u8, input: &[T; 1024]) -> Vec<u8> {
//     match (width) {
//         w @ 1  ..= 12 => {
//             let mut output = Vec::with_capacity(128 * w as usize);
//             BitPack::<T, w>::pack(input, output.as_mut_ptr());
//             output.to_vec()
//         },
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_check() {
        let threes = [3u8; 1024];
        let mut output = [0u8; 512];
        BitPack::<u8, 4>::pack(&threes, &mut output);
        // 0b00000011 packs into 0b00110011 == 51
        assert_eq!(output, [51u8; 512]);
    }
}