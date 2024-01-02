## Style Guide

### Returned memory: new vs borrowed references

Functions that return new references should always take an allocator.

This is a useful convention so the caller knows who owns the result.

### Enum Values

When an enum value is considered stable API, it should be explicitly defined. For example:

```zig
// Enum as API
const Foo = enum(u8) {
  a = 0,
  b = 1,
};

// Internal enum
const Foo = enum {
  a,
  b,
};
```

## Debugging Tips

Recommended that you use a debug build of Python:

```
pyenv install --debug 3.11.5
```

Poetry et al are not very good at choosing between 3.11.5 and 3.11.5-debug though, so I would recommend picking a different
patch version (e.g. 3.11.4) so you can switch between them.

### EXC_BAD_INSTRUCTION

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