const std = @import("std");
const py = @import("pydust");

const PyReader = struct {
    pub const Reader = std.io.Reader(*PyReader, py.PyError, read);
    underlying: py.PyObject,

    pub fn read(self: *PyReader, buffer: []u8) py.PyError!usize {
        const pybuffer = try py.PyMemoryView.fromSlice(buffer);
        defer pybuffer.decref();
        return self.underlying.call(usize, "readinto", .{pybuffer}, .{});
    }

    pub fn reader(self: *PyReader) Reader {
        return .{ .context = self };
    }
};

pub fn pythonReader(underlying: py.PyObject) !PyReader {
    if (try underlying.has("readinto")) {
        return .{ .underlying = underlying };
    } else {
        return py.TypeError.raiseFmt("Expected an object with readinto function, {} given", .{try py.str(py.type_(underlying))});
    }
}

const PyWriter = struct {
    pub const Writer = std.io.Writer(*PyWriter, py.PyError, write);
    underlying: py.PyObject,

    pub fn write(self: *PyWriter, bytes: []const u8) py.PyError!usize {
        const viewBytes = try py.PyMemoryView.fromSlice(bytes);
        defer viewBytes.decref();
        return self.underlying.call(usize, "write", .{viewBytes}, .{});
    }

    pub fn writer(self: *PyWriter) Writer {
        return .{ .context = self };
    }
};

pub fn pythonWriter(underlying: py.PyObject) !PyWriter {
    if (try underlying.has("write")) {
        return .{ .underlying = underlying };
    } else {
        return py.TypeError.raiseFmt("Expected an object with write function, {} given", .{try py.str(py.type_(underlying))});
    }
}
