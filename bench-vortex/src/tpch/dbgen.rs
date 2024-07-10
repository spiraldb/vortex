use std::path::{Path, PathBuf};

use itertools::Itertools;
use xshell::{cmd, Shell};

/// Download TPC-H data via Docker.

pub struct DBGen {
    options: DBGenOptions,
}

pub struct DBGenOptions {
    /// Scale factor of the data in GB.
    pub scale_factor: u8,

    /// Location on-disk to store generated files.
    pub base_dir: PathBuf,
}

impl Default for DBGenOptions {
    /// Generate a default options.
    ///
    /// # Panics
    ///
    /// Will panic if `std::env::current_dir()` fails with error.
    fn default() -> Self {
        Self {
            scale_factor: 1,
            base_dir: std::env::current_dir().unwrap().join("data").join("tpch"),
        }
    }
}

impl DBGenOptions {
    pub fn with_base_dir<P: AsRef<Path>>(self, dir: P) -> Self {
        Self {
            base_dir: dir.as_ref().to_owned(),
            scale_factor: self.scale_factor,
        }
    }
}

impl DBGen {
    pub fn new(options: DBGenOptions) -> Self {
        Self { options }
    }
}

impl DBGen {
    /// Generate the TPC-H data files for use with benchmarks.
    pub fn generate(&self) -> anyhow::Result<PathBuf> {
        let sh = Shell::new()?;

        let scale_factor = self.options.scale_factor.to_string();

        // mkdir -p the output directory
        let output_dir = self.options.base_dir.join(scale_factor.as_str());
        sh.create_dir(&output_dir)?;

        // See if the success file has been written. If so, do not run expensive generator
        // process again.
        let success_file = output_dir.join(".success");
        if sh.path_exists(&success_file) {
            return Ok(output_dir);
        }

        let tpch_path = output_dir.canonicalize()?.to_string_lossy().to_string();

        // Generate the files using Docker container.
        cmd!(
            sh,
            "docker run --rm -v {tpch_path}:/data ghcr.io/scalytics/tpch-docker:main -s {scale_factor} -v -f"
        )
            .run()?;

        // Every tpch .tbl file is a pipe-separated values, but for some strange reason, *also* includes
        // a trailing pipe at the end of every line.
        // DataFusion's CSV reader (and assumedly most others) do not support this, so we rewrite all the
        // files before completing generation.
        for table_path in sh.read_dir(&output_dir)? {
            clean_trailing_pipes(&sh, &table_path)?;
        }

        // Write a success file to indicate this scale-factor is created.
        sh.write_file(success_file, vec![])?;

        Ok(output_dir)
    }
}

fn clean_trailing_pipes<P: AsRef<Path>>(sh: &Shell, path: P) -> anyhow::Result<()> {
    let stripped = sh
        .read_file(&path)?
        .lines()
        .map(|line| line.strip_suffix("|").unwrap())
        .join("\n");
    sh.write_file(&path, stripped.as_bytes())?;

    Ok(())
}
