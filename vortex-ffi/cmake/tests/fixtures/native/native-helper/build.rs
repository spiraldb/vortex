// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for (cpp, source, name) in [
        (false, "native.c", "native_c"),
        (true, "native.cpp", "native_cpp"),
    ] {
        let mut build = cc::Build::new();
        build.cpp(cpp).file(source);
        assert!(build.try_get_compiler()?.is_like_clang());
        assert!(build.is_flag_supported("-fno-omit-frame-pointer")?);
        assert!(!build.is_flag_supported("-fdefinitely-not-a-supported-flag")?);
        build.compile(name);
        println!("cargo:rerun-if-changed={source}");
    }
    Ok(())
}
