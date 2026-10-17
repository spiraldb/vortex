// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Select concrete text row families before the shared visitor constructs a row loop.
//!
//! These macros repeat only typed dispatch. Semantic validation and row operations remain in the
//! calling function. Binary inputs select their layouts independently, including mixed layouts.

macro_rules! dispatch_text {
    ($host:ident, $dtype:expr, $visit:ident, [$($generics:tt)*], $($args:expr),+ $(,)?) => {
        match <$host as rowfn::TextBinding>::text_layout($dtype)? {
            rowfn::TextLayout::Offset32 => {
                $visit::<$host, <$host as rowfn::TextBinding>::Offset32, $($generics)*>($($args),+)
            }
            rowfn::TextLayout::Offset64 => {
                $visit::<$host, <$host as rowfn::TextBinding>::Offset64, $($generics)*>($($args),+)
            }
            rowfn::TextLayout::View => {
                $visit::<$host, <$host as rowfn::TextBinding>::Text, $($generics)*>($($args),+)
            }
        }
    };
}

macro_rules! dispatch_text_pair {
    ($host:ident, $lhs:expr, $rhs:expr, $visit:ident, [$($generics:tt)*], $($args:expr),+ $(,)?) => {{
        let left = <$host as rowfn::TextBinding>::text_layout($lhs)?;
        let right = <$host as rowfn::TextBinding>::text_layout($rhs)?;
        match left {
            rowfn::TextLayout::Offset32 => $crate::text_dispatch::dispatch_text_pair!(
                @right $host, <$host as rowfn::TextBinding>::Offset32, right,
                $visit, [$($generics)*], $($args),+
            ),
            rowfn::TextLayout::Offset64 => $crate::text_dispatch::dispatch_text_pair!(
                @right $host, <$host as rowfn::TextBinding>::Offset64, right,
                $visit, [$($generics)*], $($args),+
            ),
            rowfn::TextLayout::View => $crate::text_dispatch::dispatch_text_pair!(
                @right $host, <$host as rowfn::TextBinding>::Text, right,
                $visit, [$($generics)*], $($args),+
            ),
        }
    }};
    (@right $host:ident, $left:ty, $layout:expr, $visit:ident, [$($generics:tt)*], $($args:expr),+) => {
        match $layout {
            rowfn::TextLayout::Offset32 => {
                $visit::<$host, $left, <$host as rowfn::TextBinding>::Offset32, $($generics)*>(
                    $($args),+
                )
            }
            rowfn::TextLayout::Offset64 => {
                $visit::<$host, $left, <$host as rowfn::TextBinding>::Offset64, $($generics)*>(
                    $($args),+
                )
            }
            rowfn::TextLayout::View => {
                $visit::<$host, $left, <$host as rowfn::TextBinding>::Text, $($generics)*>($($args),+)
            }
        }
    };
}

pub(crate) use dispatch_text;
pub(crate) use dispatch_text_pair;
