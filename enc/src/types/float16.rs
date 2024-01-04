use bytemuck::{Pod, Zeroable};

// Take from arrow2 https://github.com/jorgecarleitao/arrow2/blob/3ddc6a10c6fbc2d0f85a9f66eeb46112abd07029/src/types/native.rs
// Consider replacing this with half-rs instead of reusing
use crate::types::{PType, PrimitiveType};

#[derive(Copy, Clone, Default, Zeroable, Pod)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct f16(pub u16);

impl PartialEq for f16 {
    #[inline]
    fn eq(&self, other: &f16) -> bool {
        if self.is_nan() || other.is_nan() {
            false
        } else {
            (self.0 == other.0) || ((self.0 | other.0) & 0x7FFFu16 == 0)
        }
    }
}

// see https://github.com/starkat99/half-rs/blob/main/src/binary16.rs
impl f16 {
    /// The difference between 1.0 and the next largest representable number.
    pub const EPSILON: f16 = f16(0x1400u16);

    #[inline]
    #[must_use]
    pub(crate) const fn is_nan(self) -> bool {
        self.0 & 0x7FFFu16 > 0x7C00u16
    }

    /// Casts from u16.
    #[inline]
    pub const fn from_bits(bits: u16) -> f16 {
        f16(bits)
    }

    /// Casts to u16.
    #[inline]
    pub const fn to_bits(self) -> u16 {
        self.0
    }

    /// Casts this `f16` to `f32`
    pub fn to_f32(self) -> f32 {
        let i = self.0;
        // Check for signed zero
        if i & 0x7FFFu16 == 0 {
            return f32::from_bits((i as u32) << 16);
        }

        let half_sign = (i & 0x8000u16) as u32;
        let half_exp = (i & 0x7C00u16) as u32;
        let half_man = (i & 0x03FFu16) as u32;

        // Check for an infinity or NaN when all exponent bits set
        if half_exp == 0x7C00u32 {
            // Check for signed infinity if mantissa is zero
            if half_man == 0 {
                let number = (half_sign << 16) | 0x7F80_0000u32;
                return f32::from_bits(number);
            } else {
                // NaN, keep current mantissa but also set most significiant mantissa bit
                let number = (half_sign << 16) | 0x7FC0_0000u32 | (half_man << 13);
                return f32::from_bits(number);
            }
        }

        // Calculate single-precision components with adjusted exponent
        let sign = half_sign << 16;
        // Unbias exponent
        let unbiased_exp = ((half_exp as i32) >> 10) - 15;

        // Check for subnormals, which will be normalized by adjusting exponent
        if half_exp == 0 {
            // Calculate how much to adjust the exponent by
            let e = (half_man as u16).leading_zeros() - 6;

            // Rebias and adjust exponent
            let exp = (127 - 15 - e) << 23;
            let man = (half_man << (14 + e)) & 0x7F_FF_FFu32;
            return f32::from_bits(sign | exp | man);
        }

        // Rebias exponent for a normalized normal
        let exp = ((unbiased_exp + 127) as u32) << 23;
        let man = (half_man & 0x03FFu32) << 13;
        f32::from_bits(sign | exp | man)
    }

    /// Casts an `f32` into `f16`
    pub fn from_f32(value: f32) -> Self {
        let x: u32 = value.to_bits();

        // Extract IEEE754 components
        let sign = x & 0x8000_0000u32;
        let exp = x & 0x7F80_0000u32;
        let man = x & 0x007F_FFFFu32;

        // Check for all exponent bits being set, which is Infinity or NaN
        if exp == 0x7F80_0000u32 {
            // Set mantissa MSB for NaN (and also keep shifted mantissa bits)
            let nan_bit = if man == 0 { 0 } else { 0x0200u32 };
            return f16(((sign >> 16) | 0x7C00u32 | nan_bit | (man >> 13)) as u16);
        }

        // The number is normalized, start assembling half precision version
        let half_sign = sign >> 16;
        // Unbias the exponent, then bias for half precision
        let unbiased_exp = ((exp >> 23) as i32) - 127;
        let half_exp = unbiased_exp + 15;

        // Check for exponent overflow, return +infinity
        if half_exp >= 0x1F {
            return f16((half_sign | 0x7C00u32) as u16);
        }

        // Check for underflow
        if half_exp <= 0 {
            // Check mantissa for what we can do
            if 14 - half_exp > 24 {
                // No rounding possibility, so this is a full underflow, return signed zero
                return f16(half_sign as u16);
            }
            // Don't forget about hidden leading mantissa bit when assembling mantissa
            let man = man | 0x0080_0000u32;
            let mut half_man = man >> (14 - half_exp);
            // Check for rounding (see comment above functions)
            let round_bit = 1 << (13 - half_exp);
            if (man & round_bit) != 0 && (man & (3 * round_bit - 1)) != 0 {
                half_man += 1;
            }
            // No exponent for subnormals
            return f16((half_sign | half_man) as u16);
        }

        // Rebias the exponent
        let half_exp = (half_exp as u32) << 10;
        let half_man = man >> 13;
        // Check for rounding (see comment above functions)
        let round_bit = 0x0000_1000u32;
        if (man & round_bit) != 0 && (man & (3 * round_bit - 1)) != 0 {
            // Round it
            f16(((half_sign | half_exp | half_man) + 1) as u16)
        } else {
            f16((half_sign | half_exp | half_man) as u16)
        }
    }
}

impl std::fmt::Debug for f16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.to_f32())
    }
}

impl std::fmt::Display for f16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_f32())
    }
}

impl PrimitiveType for f16 {
    const PTYPE: PType = PType::F16;
    type ArrowType = arrow2::types::f16;
    type Bytes = [u8; 2];
}
