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

use std::mem::{MaybeUninit, size_of};

use arrayref::array_mut_ref;
use seq_macro::seq;
use uninit::prelude::VecCapacity;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[derive(Debug)]
pub struct UnsupportedBitWidth;

pub struct Pred<const B: bool>;
pub trait Satisfied {}
impl Satisfied for Pred<true> {}

/// BitPack into a compile-time known bit-width.
pub trait BitPack<const W: u8>
where
    Self: Sized,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() as u8 }>: Satisfied,
{
    fn bitpack<'a>(
        input: &[Self; 1024],
        output: &'a mut [MaybeUninit<u8>; 128 * W as usize],
    ) -> &'a [u8; 128 * W as usize];
}

seq!(N in 1..8 {
    impl BitPack<N> for u8 {
        #[inline]
        fn bitpack<'a>(
            input: &[Self; 1024],
            output: &'a mut [MaybeUninit<u8>; 128 * N],
        ) -> &'a [u8; 128 * N] {
            unsafe {
                let output_array: &mut [u8; 128 * N] = std::mem::transmute(output);
                fl_bitpack_u8_u~N(input, output_array);
                output_array
            }
        }
    }
});

pub trait TryBitPack
where
    Self: Sized,
{
    fn try_bitpack<'a>(
        input: &[Self; 1024],
        width: u8,
        output: &'a mut [MaybeUninit<u8>],
    ) -> Result<&'a [u8], UnsupportedBitWidth>;

    fn try_bitpack_into<'a>(
        input: &[Self; 1024],
        width: u8,
        output: &mut Vec<u8>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_bitpack(input, width, output.reserve_uninit(width as usize * 128))?;
        unsafe { output.set_len(output.len() + (width as usize * 128)) }
        Ok(())
    }
}

impl TryBitPack for u8 {
    fn try_bitpack<'a>(
        input: &[Self; 1024],
        width: u8,
        output: &'a mut [MaybeUninit<u8>],
    ) -> Result<&'a [u8], UnsupportedBitWidth> {
        seq!(N in 1..8 {
            match width {
                #(N => Ok(BitPack::<N>::bitpack(input, array_mut_ref![output, 0, N * 128]).as_slice()),)*
                _ => Err(UnsupportedBitWidth),
            }
        })
    }
}

//
// // TODO(ngates): export packed widths in the C API so we can check for support here.
// trait BitPack<T: FLUnsigned, const W: usize> where Pred<{ W > 0 }>: Satisfied, Pred<{ W < size_of::<T>() * 8 }>: Satisfied {
//     fn pack(self: Self, output: &mut [T; 128 * W]);
// }
//
// macro_rules! impl_bitpack {
//     ($t:ty) => {
//         paste::item! {
//             seq!(W in 1..size_of!($t) {
//                 impl<'a> BitPack<$t, W> for &'a[$t; 1024]  {
//                     fn pack(self: &'a[$t; 1024], output: &mut [$t; 128 * W])  {
//                         unsafe { [<fl_bitpack_ $t _u>]~W(self, output) }
//                     }
//                 }
//             });
//             //
//             // fn [<bitpack_ $t>](width: u8, input: &[T; 1024]) -> Vec<u8> {
//             //     match (width) {
//             //         seq!(W in 1..size_of::<$t>() * 8 {
//             //            W => {
//             //                 let mut output = [0u8; 128 * W];
//             //                 BitPack::<$t, W>::pack(input, &mut output);
//             //                 output.to_vec()
//             //            }
//             //         });
//             //         w @ 1  ..= 12 => {
//             //             let mut output = [0u8; 128 * w as usize];
//             //             BitPack::<T, w>::pack(input, &mut output);
//             //             output.to_vec()
//             //         },
//             //     }
//             // }
//         }
//     }
// }
//
// impl_bitpack!(u8);
// // impl_bitpack!(u16);
// // impl_bitpack!(u32);
// // impl_bitpack!(u64);
//
// // fn bitpack<T: FLUnsigned>(width: u8, input: &[T; 1024]) -> Vec<u8> {
// //     match (width) {
// //         w @ 1  ..= 12 => {
// //             let mut output = Vec::with_capacity(128 * w as usize);
// //             BitPack::<T, w>::pack(input, output.as_mut_ptr());
// //             output.to_vec()
// //         },
// //     }
// // }
//
// #[cfg(test)]
// mod tests {
//     #[test]
//     fn sanity_check() {
//         let threes = [3u8; 1024];
//         let mut output = [0u8; 512];
//         BitPack::<u8, 4>::pack(&threes, &mut output);
//         // 0b00000011 packs into 0b00110011 == 51
//         assert_eq!(output, [51u8; 512]);
//     }
// }
