use std::ffi::OsStr;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::prelude::OsStrExt;

#[cfg(not(unix))]
compile_error!("Unsupported platform!");

pub struct DigestPathGenerator<const N: usize> {
    subdir_levels: usize,
}

impl<const N: usize> DigestPathGenerator<N> {
    pub fn new(subdir_levels: usize) -> Self {
        assert!(subdir_levels < N);

        DigestPathGenerator { subdir_levels }
    }

    fn splice_2d_array<'a>(src: &'a mut [[u8; N]; 2]) -> &mut [u8] {
        // Because of limitations on const generics (cannot be used in
        // const expressions on stable Rust), we have to splice the
        // two N-byte sized arrays into a single slice over both.
        unsafe { std::slice::from_raw_parts_mut(src.as_mut_ptr() as *mut u8, N * 2) }
    }

    pub fn get_path<P: AsRef<Path>>(&self, base_path: P, digest: &[u8; N]) -> PathBuf {
        // Temporary mutable buffer to hold hex-encoded digest
        //
        // TODO: When const generic expressions are stable, make this
        // create a [u8; N * 2] array.
        let mut hex_digest_buf: [[u8; N]; 2] = [[0; N]; 2];
        let hex_digest_buf_slice = Self::splice_2d_array(&mut hex_digest_buf);

        // First, clone the base path into a new path buf
        let mut path = base_path.as_ref().to_path_buf();

        // Then, we try to predict the additional space we need to
        // allocate for the remaining path, based on the digest length
        // and the number of subdirectory levels:
        path.reserve(
            // Include the base_path space,
            base_path.as_ref().as_os_str().len()
	    // the intial path seperator (1 UTF-8 1-byte char),
		+ 1
	    // for each subdir level a path seperator (1 UTF-8 1-byte
	    // char) and hex-encoded byte (2 UTF-8 1-byte chars),
		+ (3 * self.subdir_levels)
	    // as well as the final hex-encoded digest (2 UTF-8 1-byte
	    // chars per byte):
		+ (2 * N),
        );

        // Hex-encode the digest into the provided buffer:
        hex::encode_to_slice(digest, hex_digest_buf_slice).unwrap();

        // Finally, push the subdirectories and file name onto the
        // base path:
        for subdir_level in 0..self.subdir_levels {
            path.push(<OsStr as OsStrExt>::from_bytes(
                &hex_digest_buf_slice[(2 * subdir_level)..(2 * (subdir_level + 1))],
            ));
        }
        path.push(<OsStr as OsStrExt>::from_bytes(hex_digest_buf_slice));

        path
    }
}
