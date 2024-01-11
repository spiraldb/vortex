const std = @import("std");
const py = @import("pydust");
const trazy = @import("trazy");

pub const PyTrazyCtx = py.class(struct {
    const Self = @This();

    wrapped: trazy.Ctx,

    pub fn __enter__(self: *const Self) *const Self {
        py.incref(self);
        return self;
    }

    pub fn __exit__(self: *const Self) bool {
        self.wrapped.end();
        // false tells interpreter that any exception inside context manager should be propagated
        return false;
    }

    pub fn end(self: *const Self) void {
        self.wrapped.end();
    }

    pub fn addText(self: *const Self, args: struct { text: py.PyString }) !void {
        args.text.incref();
        self.wrapped.addText(try args.text.asSlice());
    }

    pub fn setName(self: *const Self, args: struct { name: py.PyString }) !void {
        args.name.incref();
        self.wrapped.setName(try args.name.asSlice());
    }

    pub fn setColor(self: *const Self, args: struct { color: u32 }) void {
        self.wrapped.setColor(args.color);
    }

    pub fn setValue(self: *const Self, args: struct { value: u64 }) void {
        self.wrapped.setValue(args.value);
    }

    pub usingnamespace py.zig(struct {
        pub fn wrapOwned(ctx: trazy.Ctx) py.PyError!*const Self {
            return py.init(Self, .{ .wrapped = ctx });
        }
    });
});

pub fn trace(args: struct { name: ?py.PyString = null }) !*const PyTrazyCtx {
    const frameObj = py.PyFrame.get().?;
    const codeObj = frameObj.code();
    defer codeObj.obj.decref();
    const fnName = try codeObj.name();
    defer fnName.decref();
    const fileName = try codeObj.fileName();
    defer fileName.decref();
    const location: std.builtin.SourceLocation = .{
        .file = try fileName.asSlice(),
        .fn_name = try fnName.asSlice(),
        .line = frameObj.lineNumber(),
        .column = 0,
    };
    if (args.name) |name| {
        return try PyTrazyCtx.wrapOwned(trazy.traceNamedAlloc(location, try name.asSlice()));
    } else {
        return try PyTrazyCtx.wrapOwned(trazy.traceAlloc(location));
    }
}
