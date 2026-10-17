// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! SQL pattern and regular-expression predicates shared by both row hosts.
//!
//! Constant patterns and distinct valid array patterns are compiled once per invocation.
//! Preparation borrows only valid decoded rows and retains independently owned matchers.

use std::collections::HashMap;

use memchr::memmem;
use regex::{Regex, RegexBuilder};
use rowfn::BatchBinding;
use rowfn::BooleanOutput;
use rowfn::Host;
use rowfn::HostResult;
use rowfn::InputBinding;
use rowfn::RowFn;
use rowfn::RowKind;
use rowfn::RowVisitor;
use rowfn::TextBinding;
use rowfn::TextValue;

use crate::NoOptions;
use crate::text_dispatch::dispatch_text_pair;

/// SQL LIKE or ILIKE with optional result negation.
///
/// `%` matches zero or more Unicode characters, `_` matches one Unicode character, and a
/// backslash quotes the following pattern character. Both inputs follow strict null propagation.
/// Invalid patterns on valid rows return an error.
#[derive(Clone, Copy)]
pub struct Like<const CASE_INSENSITIVE: bool, const NEGATED: bool>;

impl<H, const CASE_INSENSITIVE: bool, const NEGATED: bool> RowFn<H>
    for Like<CASE_INSENSITIVE, NEGATED>
where
    H: BatchBinding + TextBinding + BooleanOutput,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["text", "pattern"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        let kind = if CASE_INSENSITIVE {
            PatternKind::ILike
        } else {
            PatternKind::Like
        };

        dispatch_text_pair!(H, &args[0], &args[1], dispatch_pattern, [V], kind, NEGATED, visitor)
    }
}

/// Match a regular expression against each valid input string.
///
/// This is the two-argument form of Arrow's `regexp_is_match`, without its optional flags input.
/// Invalid patterns on valid rows return an error. An empty pattern matches every valid string.
#[derive(Clone, Copy)]
pub struct RegexpIsMatch;

impl<H> RowFn<H> for RegexpIsMatch
where
    H: BatchBinding + TextBinding + BooleanOutput,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["text", "pattern"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_text_pair!(
            H, &args[0], &args[1], dispatch_pattern, [V], PatternKind::Regex, false, visitor,
        )
    }
}

#[derive(Clone, Copy)]
enum PatternKind {
    Like,
    ILike,
    Regex,
}

impl PatternKind {
    fn compile<H: Host>(self, pattern: &str) -> HostResult<H, Matcher> {
        Matcher::compile(self, pattern).map_err(|error| {
            let operation = match self {
                Self::Like => "LIKE pattern could not compile",
                Self::ILike => "ILIKE pattern could not compile",
                Self::Regex => "regular expression did not compile",
            };
            H::error(&format!("{operation}: {error}"))
        })
    }
}

enum MatcherCache {
    Empty,
    Constant(Matcher),
    Varying(HashMap<String, Matcher>),
}

impl MatcherCache {
    fn get(&self, pattern: &str) -> Option<&Matcher> {
        match self {
            Self::Empty => None,
            Self::Constant(matcher) => Some(matcher),
            Self::Varying(cache) => cache.get(pattern),
        }
    }
}

enum SimpleKind {
    Equal,
    Prefix,
    Suffix,
    Contains,
}

fn dispatch_pattern<H, Left, Right, V>(
    kind: PatternKind,
    negated: bool,
    visitor: V,
) -> HostResult<H, V::VisitResult>
where
    H: BatchBinding + InputBinding<Left> + InputBinding<Right> + BooleanOutput,
    Left: RowKind,
    Right: RowKind,
    for<'a> Left::Value<'a>: TextValue,
    for<'a> Right::Value<'a>: TextValue,
    V: RowVisitor<H>,
{
    visitor.visit_batch_prepared_deferred_bool::<(Left, Right), _, bool, false>(
        move |rows| {
            if rows.valid_len() == 0 {
                return Ok(MatcherCache::Empty);
            }
            if let Some(pattern) = rows.constants().and_then(|constants| constants.1) {
                return kind.compile::<H>(pattern.as_str()).map(MatcherCache::Constant);
            }

            let mut cache = HashMap::new();
            rows.try_for_each_valid(|_, (_, pattern)| {
                let pattern = pattern.as_str();
                if !cache.contains_key(pattern) {
                    cache.insert(pattern.to_owned(), kind.compile::<H>(pattern)?);
                }
                Ok(())
            })?;
            Ok(MatcherCache::Varying(cache))
        },
        move |prepared, (text, pattern)| {
            prepared.get(pattern.as_str()).map_or((false, true), |matcher| {
                (matcher.matches(&text) != negated, false)
            })
        },
        |failed| {
            if failed {
                Err(H::error("prepared pattern cache lost a valid row"))
            } else {
                Ok(())
            }
        },
    )
}

enum Matcher {
    Equal(String),
    Prefix(String),
    Suffix(String),
    Contains(memmem::Finder<'static>),
    InsensitiveEqual(String, Regex),
    InsensitivePrefix(String, Regex),
    InsensitiveSuffix(String, Regex),
    Regex(Regex),
}

impl Matcher {
    fn compile(kind: PatternKind, pattern: &str) -> Result<Self, regex::Error> {
        match kind {
            PatternKind::Regex => Regex::new(pattern).map(Self::Regex),
            PatternKind::Like => Self::compile_like(pattern, false),
            PatternKind::ILike => Self::compile_like(pattern, true),
        }
    }

    fn compile_like(pattern: &str, insensitive: bool) -> Result<Self, regex::Error> {
        let plain = |part: &str| !part.bytes().any(|b| matches!(b, b'%' | b'_' | b'\\'));
        let simple = if plain(pattern) {
            Some((SimpleKind::Equal, pattern))
        } else if let Some(part) = pattern.strip_suffix('%').filter(|part| plain(part)) {
            Some((SimpleKind::Prefix, part))
        } else if let Some(part) = pattern.strip_prefix('%').filter(|part| plain(part)) {
            Some((SimpleKind::Suffix, part))
        } else if let Some(part) = pattern.strip_prefix('%').and_then(|part| part.strip_suffix('%'))
            .filter(|part| plain(part))
        {
            Some((SimpleKind::Contains, part))
        } else {
            None
        };

        if let Some((operation, part)) = simple {
            if insensitive && pattern.is_ascii() {
                let fallback = Self::compile_regex_like(pattern, true)?;
                return Ok(match operation {
                    SimpleKind::Equal => Self::InsensitiveEqual(part.to_owned(), fallback),
                    SimpleKind::Prefix => Self::InsensitivePrefix(part.to_owned(), fallback),
                    SimpleKind::Suffix => Self::InsensitiveSuffix(part.to_owned(), fallback),
                    SimpleKind::Contains => Self::Regex(fallback),
                });
            }

            if !insensitive {
                return Ok(match operation {
                    SimpleKind::Equal => Self::Equal(part.to_owned()),
                    SimpleKind::Prefix => Self::Prefix(part.to_owned()),
                    SimpleKind::Suffix => Self::Suffix(part.to_owned()),
                    SimpleKind::Contains => {
                        Self::Contains(memmem::Finder::new(part.as_bytes()).into_owned())
                    }
                });
            }
        }

        Self::compile_regex_like(pattern, insensitive).map(Self::Regex)
    }

    fn compile_regex_like(pattern: &str, insensitive: bool) -> Result<Regex, regex::Error> {
        let mut expression = String::with_capacity(pattern.len());
        let mut chars = pattern.chars().peekable();
        if chars.peek() == Some(&'%') {
            chars.next();
        } else {
            expression.push('^');
        }

        while let Some(ch) = chars.next() {
            match ch {
                '\\' => {
                    if let Some(next) = chars.next() {
                        if regex_syntax::is_meta_character(next) {
                            expression.push('\\');
                        }
                        expression.push(next);
                    } else {
                        expression.push_str("\\\\");
                    }
                }
                '%' => expression.push_str(".*"),
                '_' => expression.push('.'),
                _ => {
                    if regex_syntax::is_meta_character(ch) {
                        expression.push('\\');
                    }
                    expression.push(ch);
                }
            }
        }

        if expression.ends_with(".*") {
            expression.truncate(expression.len() - 2);
        } else {
            expression.push('$');
        }

        RegexBuilder::new(&expression)
            .case_insensitive(insensitive)
            .dot_matches_new_line(true)
            .build()
    }

    fn matches(&self, text: &impl TextValue) -> bool {
        match self {
            Self::Equal(value) => text.as_str() == value,
            Self::Prefix(value) => text.prefix(value.len()) == value.as_bytes(),
            Self::Suffix(value) => text.suffix(value.len()) == value.as_bytes(),
            Self::Contains(finder) => finder.find(text.as_str().as_bytes()).is_some(),
            Self::InsensitiveEqual(value, fallback) => {
                let text = text.as_str();
                if text.is_ascii() { text.eq_ignore_ascii_case(value) } else { fallback.is_match(text) }
            }
            Self::InsensitivePrefix(value, fallback) => {
                let text = text.as_str();
                if text.is_ascii() { ascii_prefix(text, value) } else { fallback.is_match(text) }
            }
            Self::InsensitiveSuffix(value, fallback) => {
                let text = text.as_str();
                if text.is_ascii() { ascii_suffix(text, value) } else { fallback.is_match(text) }
            }
            Self::Regex(regex) => regex.is_match(text.as_str()),
        }
    }
}

fn ascii_prefix(text: &str, prefix: &str) -> bool {
    text.as_bytes().get(..prefix.len())
        .is_some_and(|part| part.eq_ignore_ascii_case(prefix.as_bytes()))
}

fn ascii_suffix(text: &str, suffix: &str) -> bool {
    text.as_bytes().get(text.len().saturating_sub(suffix.len())..)
        .is_some_and(|part| part.eq_ignore_ascii_case(suffix.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{Matcher, PatternKind};

    #[test]
    fn like_escapes_and_unicode_wildcards() -> Result<(), regex::Error> {
        for (pattern, input, expected) in [
            (r"a\%b", "a%b", true),
            (r"a\_b", "a_b", true),
            (r"a\\%", "a\\tail", true),
            ("a_b", "aλb", true),
            ("a_b", "ab", false),
            ("%tail%", "head\ntail", true),
        ] {
            assert_eq!(Matcher::compile(PatternKind::Like, pattern)?.matches(&input), expected);
        }
        Ok(())
    }

    #[test]
    fn ilike_uses_unicode_regex_when_text_is_not_ascii() -> Result<(), regex::Error> {
        let matcher = Matcher::compile(PatternKind::ILike, "k%")?;
        assert!(matcher.matches(&"Kite"));
        assert!(matcher.matches(&"Kite"));
        Ok(())
    }
}
