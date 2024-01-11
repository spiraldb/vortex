//! Experiments in static dispatch.
//! See https://github.com/ziglang/zig/issues/17198
const std = @import("std");

pub fn assertImplements(value: anytype, comptime Interface: type) void {
    const T = @TypeOf(value);
    const info = @typeInfo(Interface).Struct;
    inline for (info.fields) |field| {
        // For now, we only check for functions.
        switch (@typeInfo(field.type)) {
            .Fn => |fn_info| {
                if (!@hasDecl(T, field.name)) {
                    @compileError(std.fmt.comptimePrint(
                        "{} missing {s}: {} from static trait {}",
                        .{ T, field.name, field.type, Interface },
                    ));
                }

                const F = @TypeOf(@field(T, field.name));
                if (!comptime matchesFn(@typeInfo(F).Fn, fn_info)) {
                    @compileError(std.fmt.comptimePrint(
                        "Expected {} function {s}: {}, but found {} function {}",
                        .{ Interface, field.name, field.type, T, F },
                    ));
                }
            },
            else => @compileError("Interface " ++ @typeName(Interface) ++ " can only contain functions. Found " ++ @typeName(field.type)),
        }
    }
}

/// We check if a static implementation matches the interface - ignoring any "self" parameter.
fn matchesFn(comptime impl: std.builtin.Type.Fn, comptime interface: std.builtin.Type.Fn) bool {
    if (impl.return_type != interface.return_type) return false;
    inline for (impl.params[1..], interface.params[1..]) |implParam, interfaceParam| {
        if (!std.meta.eql(implParam, interfaceParam)) return false;
    }
    return true;
}

test "writer" {
    // Example static trait defined as a comptime vtable (note: not function pointers).
    const IWriter = struct {
        print: fn (*anyopaque, string: []const u8) []const u8,
    };

    const IdentityWriter = struct {
        pub fn print(self: @This(), string: []const u8) []const u8 {
            _ = self;
            return string;
        }
    };

    const Closure = struct {
        fn writeSomething(writer: anytype) []const u8 {
            assertImplements(writer, IWriter);
            return writer.print("Something");
        }
    };

    const i = IdentityWriter{};
    try std.testing.expectEqualStrings("Something", Closure.writeSomething(i));
}
