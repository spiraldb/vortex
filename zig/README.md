# Spiral Zig

This contains our lowest-level and most performance sensitive code. It is written in [Zig](https://ziglang.org) because of several key language features:

1. Ultra-granular management of memory allocation
2. Comptime is fantastic for producing easy-to-read branchless & type-specialized code
3. Excellent cross-compilation support, including relatively portable SIMD via the Vector API

There are two main subprojects: zimd (platform-portable SIMD utilities) and codecz (high performance, mostly vectorized encoding/decoding functions).
Codecz depends on zimd, and is wrapped up in a static library that is then called from Rust via the C ABI.

The C headers and C ABI-friendly function wrappers live in the `c-abi` subproject. Rust bindings are generated in the codecz-sys
crate based on the contents of `c-abi/wrapper.h`.

Note: zig subprojects with the suffix `-legacy` are not built/compiled and should be deleted in the near future. They are there for
reference as we port functionality to Rust.

Note: there is also a currently-unused subproject called `trazy`, which wraps up tracing utilities. We are not currently using it, 
but should do so in the future.

## Zig Tips

### Returned memory: new vs borrowed references

Memory ownership is user-managed in Zig, so we have several a useful conventions so that a caller knows who owns the result:

1. Functions that return new references should always take an allocator.
2. If a function is a factory method for the containing struct type that takes an allocator and returns a pointer (i.e., it allocates
   the struct on the heap), then its name should start with `alloc`.
3. Functions that relinquish pre-existing owned memory to the caller should be named accordingly (e.g., `toOwnedSlice`).
4. Factory methods & initializers that take ownership of provided memory should be named accordingly (e.g., `allocWithOwnedSlice` takes
   an allocator and a slice, and allocates a new object on the heap).

### Returning a pointer to the stack

Zig lets you do this. This is very bad. Anecdotally, this most often happens in degenerate/fallback cases of functions that typically perform heap allocation,
especially when the return type is a slice. In these cases, it's very easy/tempting to return a reference to an anonymous struct (on the stack), which leads 
to stack corruption. For example, this will compile, and it will absolutely wreck you if you call if with `length <= 100`:

```zig
pub const ArraySlice = struct {
    start: usize,
    stop: usize,
};

fn sampleArrayOfLength(gpa: std.mem.Allocator, length: usize) ![]const ArraySlice {
    const sampleSize: u64 = 100;
    if (sampleSize >= length) {
        return &.{ArraySlice{ .start = 0, .stop = length }};
    }
    ...
}
```

Don't do this. If your function is supposed to ever allocate on the heap, it should *always* allocate on the heap unless it returns an `error`.

### Beware aliasing

Zig can pass *anything* by reference. Zig may pass arbitrarily complex parameters into a function by reference (Parameter Reference Optimization),
at the compiler's discretion. It may also pass the result location by reference (Result Location Semantics) at the compiler's discretion.
Especially in combination, this can lead to extremely unintuitive behavior. See the discussion on [lobste.rs](https://lobste.rs/s/et3ivs/zig_may_pass_anything_by_reference#c_yvfrnq)
or this talk from [Software You Can Love 2023](https://www.youtube.com/watch?v=dEIsJPpCZYg).

For example, the following program (modified from the lobste.rs thread) will not do what you'd expect because of PRO:

```zig
const std = @import("std");

const Foo = struct {
    bar: u32,
};

fn mutateSecondPrintFirst(a: Foo, b: *Foo) void {
  b.*.bar = 5;
  std.debug.print("wtf: {}", .{ a.bar });
}

pub fn main() !void {
    var f: Foo = Foo { .bar = 0 };
    mutateSecondPrintFirst(f, &f);
}
```

The above will print `"wtf: 5"` on zig 0.11.0. Try it out at [https://zig-play.dev](https://zig-play.dev).

This can get more insidious in cases where the zig compiler passes the *return location* by reference (so the stack variable that is returned is
actually located in the caller's stack frame), which can make code of the following shape dangerous (since the aliasing is
completely implicit, whereas the previous example was a bit more explicit). The YouTube video above provides a detailed example.


### Debugging EXC_BAD_INSTRUCTION

```
* thread #1, queue = 'com.apple.main-thread', stop reason = EXC_BAD_INSTRUCTION (code=1, subcode=0x4a03000)
  * frame #0: 0x000000012b947568 libarrow.1300.dylib`_armv8_sve_probe
    frame #1: 0x000000012b947bc0 libarrow.1300.dylib`OPENSSL_cpuid_setup + 600
    frame #2: 0x000000019d5841d8 dyld`invocation function for block in dyld4::Loader::findAndRunAllInitializers(dyld4::RuntimeState&) const::$_0::operator()() const + 168
```

This is fake news. Basically LLDB is stopping at this exception, but the program would normally continue.
Configure LLDB with:

```
settings set platform.plugin.darwin.ignored-exceptions EXC_BAD_INSTRUCTION
process handle SIGILL -n false -p true -s false
```

This can be placed in your `~/.lldbinit` file

See: https://stackoverflow.com/questions/74059978/why-is-lldb-generating-exc-bad-instruction-with-user-compiled-library-on-macos