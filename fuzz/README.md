
# Vortex Fuzz

This crate contains general fuzzing infrastructure and tooling for all public components of Vortex.

## Setup

Currently, the only thing required to run the fuzzing targets is [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz)

## Reproduce crash from CI

In the case of a crash in the nightly run, you can download the crash artifact and run `cargo-fuzz` with the exact same input with the command `cargo fuzz run fuzz_target_1 <path/to/artifact>`
