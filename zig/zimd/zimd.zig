const builtin = @import("builtin");
const std = @import("std");

pub const baselineCpu = std.Target.Cpu.baseline(builtin.cpu.arch);

pub usingnamespace @import("tblz.zig");
