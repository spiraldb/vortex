use std::ffi::c_int;

extern "C" {
    fn c_library_export() -> c_int;
}

fn main() {
    unsafe {
        // Dynamically load the built library...?
        c_library_export();
    }
}
