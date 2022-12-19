use log;

use std::ffi::OsStr;
use std::io::{Error as IOError, ErrorKind as IOErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::prelude::OsStrExt;

#[cfg(not(unix))]
compile_error!("Unsupported platform!");

use tokio::fs as async_fs;
use tokio::io::{self as async_io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time as async_time;

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

const SHARD_STORE_LOCK_ATTEMPTS: usize = 32;
const SHARD_STORE_LOCK_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

#[derive(Copy, Clone, Debug)]
pub struct LockIdentity {
    process_id: u32,
}

impl LockIdentity {
    pub fn own() -> Self {
        LockIdentity {
            process_id: std::process::id(),
        }
    }

    async fn async_deserialize(
        reader: &mut (impl AsyncRead + std::marker::Unpin),
    ) -> Result<Self, IOError> {
        let mut lock_pid = [0_u8; 4];
        reader.read_exact(&mut lock_pid).await?;

        Ok(LockIdentity {
            process_id: u32::from_be_bytes(lock_pid),
        })
    }

    async fn async_serialize(
        &self,
        writer: &mut (impl AsyncWrite + std::marker::Unpin),
    ) -> Result<(), IOError> {
        writer
            .write_all(&u32::to_be_bytes(self.process_id))
            .await
            .map(|_| ())
    }
}

impl std::fmt::Display for LockIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LockIdentity(pid: {})", self.process_id)
    }
}

// This also takes N as a const parameter. In future versions of the
// `ShardStore`, we may want to enable lazy integrity checks by re-hasing the
// file as it is read and reporting results back to the store. We can do that
// transparently to the user, as we only provide an `&mut impl AsyncRead`. For
// this we need to know the length of the digest though.
pub struct Shard<'s, const N: usize> {
    store: &'s ShardStore<N>,
    inner: Option<async_fs::File>,
}

impl<'s, const N: usize> Drop for Shard<'s, N> {
    fn drop(&mut self) {
        self.store.get_shard_close(self.inner.take().unwrap())
    }
}

impl<'s, const N: usize> AsyncRead for Shard<'s, N> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut async_io::ReadBuf<'_>,
    ) -> std::task::Poll<Result<(), IOError>> {
        std::pin::Pin::new(self.inner.as_mut().unwrap()).poll_read(cx, buf)
    }
}

pub struct InsertionShard<'s, const N: usize> {
    store: &'s ShardStore<N>,
    inner: Option<(PathBuf, async_fs::File)>,
}

impl<'s, const N: usize> InsertionShard<'s, N> {
    pub fn as_async_writer<'a>(&'a mut self) -> &'a mut (impl AsyncWrite + 'a) {
        &mut self.inner.as_mut().unwrap().1
    }

    pub async fn finalize(self, digest: &[u8; N]) -> Result<(), IOError> {
        self.store.insert_shard_finalize(self, digest).await
    }

    // Required given that we don't have an `AsyncDrop` trait.
    pub async fn abort(self) -> Result<(), IOError> {
        self.store.insert_shard_abort(self).await
    }
}

impl<'s, const N: usize> Drop for InsertionShard<'s, N> {
    fn drop(&mut self) {
        if self.inner.is_some() {
            log::error!("An instance of InsertionShard instance must not be dropped.");
        }
    }
}

pub struct ShardStore<const N: usize> {
    base_path: PathBuf,
    digest_path_gen: DigestPathGenerator<N>,
    tempfile_suffix_counter: AtomicUsize,
}

impl<const N: usize> ShardStore<N> {
    pub async fn new<P: AsRef<Path>>(
        base_path: P,
        force_unlock: bool,
        new_store_subdir_levels: usize,
    ) -> Result<Self, Result<LockIdentity, IOError>> {
        // Create the base_path directory, if it does not exit yet.
        async_fs::create_dir_all(base_path.as_ref())
            .await
            .map_err(|ioerr| Err(ioerr))?;

        // Try to acquire the shard store lock:
        let mut lock_file_path = base_path.as_ref().to_path_buf();
        lock_file_path.push("lock");

        let mut lock_res: Result<LockIdentity, Result<LockIdentity, IOError>> =
            Err(Err(IOErrorKind::Other.into()));
        // let mut _err = None;
        for _ in 0..SHARD_STORE_LOCK_ATTEMPTS {
            // Depending on the lock file policy, try to open a (new)
            // lock file and write the PID to it.
            let lock_file_acq_res = async_fs::OpenOptions::new()
                // Need to write our PID to the file:
                .write(true)
                // Don't need to read:
                .read(false)
                // If `force_unlock` is true, create and potentially
                // overwrite an existing file:
                .create(force_unlock)
                // If `force_unlock` is false, create, but never
                // overwrite an existing file:
                .create_new(!force_unlock)
                // Lock file path:
                .open(&lock_file_path)
                .await;

            // Depending on the result we have acquired the lock, need
            // to abort, or need to retry:
            match lock_file_acq_res {
                // Opening the lock file worked, we've acquired the lock. Write
                // the PID to the file now, such that other instances can
                // determine whether the process is still alive:
                Ok(mut lock_file_handle) => {
                    let lock_id = LockIdentity::own();
                    lock_id
                        .async_serialize(&mut lock_file_handle)
                        .await
                        .map_err(|ioerr| Err(ioerr))?;

                    // We've successfully claimed the lock, set error to None
                    // and break out of the loop:
                    lock_file_handle.flush().await.map_err(|ioerr| Err(ioerr))?;
                    std::mem::drop(lock_file_handle);
                    lock_res = Ok(lock_id);
                    break;
                }

                // We could not open the file (perhaps because there already
                // exists a file, or we dont't have the right permissions).
                // Check whether we can open the file in read-only mode and see
                // whether we can take over the lock, otherwise bubble up the
                // error:
                Err(lock_file_acq_err) => {
                    // Store the error we've encountered. We continue the loop
                    // below. As the error is not `Clone`, extract the
                    // `ErrorKind` first. The error serves as a marker for
                    // whether we were able to claim the lock:
                    let lock_file_acq_err_kind = lock_file_acq_err.kind();
                    lock_res = Err(Err(lock_file_acq_err));

                    // Check the nature of the error. If we have insufficient
                    // permissions, break out of the loop early:
                    if let IOErrorKind::PermissionDenied = lock_file_acq_err_kind {
                        break;
                    }

                    // Otherwise, try to open a (potentially existing lock file)
                    let lock_file_read_res = async_fs::OpenOptions::new()
                        // Just need to read, never create:
                        .read(true)
                        .write(false)
                        .create(false)
                        .create_new(false)
                        .open(&lock_file_path)
                        .await;

                    match lock_file_read_res {
                        Err(lock_file_read_err) => {
                            // If we can't open this file because it does not exist, it
                            // might've just been deleted. Try again:
                            if let IOErrorKind::NotFound = lock_file_read_err.kind() {
                                continue;
                            } else {
                                // For all other errors, return early:
                                break;
                            }
                        }

                        Ok(mut lock_file_read_handle) => {
                            // Try to parse a LockIdentity from the file:
                            let lock_id_parse_res =
                                LockIdentity::async_deserialize(&mut lock_file_read_handle).await;

                            // In either case, close the opened file handle over
                            // the lock file:
                            lock_file_read_handle
                                .flush()
                                .await
                                .map_err(|ioerr| Err(ioerr))?;
                            std::mem::drop(lock_file_read_handle);

                            match lock_id_parse_res {
                                // If we've managed to read the PID, check
                                // whether the process is still running:
                                Ok(lock_id) => {
                                    // TODO: this should check whether the
                                    // current LockIdentity is still valid
                                    // (i.e. the process is still running). For
                                    // now, assume it is and return an error:
                                    lock_res = Err(Ok(lock_id));
                                    break;
                                }

                                Err(lock_id_parse_err) => {
                                    // In case we didn't read enough data, we
                                    // might be racing with another process
                                    // trying to acquire the lock. In this case,
                                    // retry after a timeout:
                                    if let IOErrorKind::UnexpectedEof = lock_id_parse_err.kind() {
                                        async_time::sleep(SHARD_STORE_LOCK_RETRY_DELAY).await;
                                    } else {
                                        // For all other errors, return early:
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Return if we didn't manage to acquire a lock:
        lock_res?;

        // TODO: if we operate on an existing shard store, read the
        // subdir-levels from its configuration:
        let subdir_levels = new_store_subdir_levels;

        Ok(ShardStore {
            base_path: base_path.as_ref().to_path_buf(),
            digest_path_gen: DigestPathGenerator::<N>::new(subdir_levels),
            tempfile_suffix_counter: AtomicUsize::new(0),
        })
    }

    pub async fn insert_shard_by_writer<'a>(&'a self) -> Result<InsertionShard<'a, N>, IOError> {
        // Create a directory for temporary partial shards:
        let mut temp_shards_dir = self.base_path.clone();
        temp_shards_dir.push("temp");
        async_fs::create_dir(&temp_shards_dir)
            .await
            .or_else(|ioerr| match ioerr.kind() {
                IOErrorKind::AlreadyExists => Ok(()),
                _ => Err(ioerr),
            })?;

        // Loop until we found a suitable temporary file path:
        let mut temp_file = None;
        while temp_file.is_none() {
            // Generate a file name based on the current SystemTime, as well as
            // a "random" suffix (currently we just count). This will ensure
            // that two files at the same SystemTime will _very likely_ have a
            // different file name. Having the prefix be the system time makes
            // the naming scheme understandable and predictable to a human
            // operator.
            let prefix = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("SystemTime must be equal or later than UNIX_EPOCH.")
                .as_secs();
            let suffix = self.tempfile_suffix_counter.fetch_add(1, Ordering::Relaxed) + 1;

            let mut file_name = temp_shards_dir.clone();
            file_name.push(format!("temp.{}.{}", prefix, suffix));

            // Try to create a new file under the above filename. If this file
            // already exists, repeat.
            let open_res = async_fs::OpenOptions::new()
                .read(false)
                .write(true)
                .create_new(true)
                .open(&file_name)
                .await;

            match open_res {
                Ok(file_handle) => {
                    temp_file = Some((file_name, file_handle));
                }

                Err(err) => {
                    if let IOErrorKind::AlreadyExists = err.kind() {
                        // Fine, continue the loop until we find a path which
                        // does not exist.
                    } else {
                        // Bubble up the error.
                        Err(err)?
                    }
                }
            }
        }

        // After this loop, temp_file is guaranteed to be Some(_):
        assert!(temp_file.is_some());

        Ok(InsertionShard {
            store: self,
            inner: temp_file,
        })
    }

    async fn insert_shard_finalize<'a>(
        &'a self,
        mut insertion_shard: InsertionShard<'a, N>,
        digest: &[u8; N],
    ) -> Result<(), IOError> {
        // By construction, the [`InsertionShard`] may only call this method on
        // its own instance of the [`ShardStore`]
        assert!(self as *const _ == insertion_shard.store as *const _);

        // We consume and then immediately drop the struct, and never set
        // `InsertionShard.inner` to `None` otherwise, so this must succeed:
        let (tmppath, mut tmpfile) = insertion_shard.inner.take().unwrap();
        std::mem::drop(insertion_shard);

        // Flush and close the temporary file:
        tmpfile.flush().await?;
        std::mem::drop(tmpfile);

        // Determine the final shard path and move the file:
        let shard_path = self.digest_path_gen.get_path(&self.base_path, digest);
        async_fs::create_dir_all(&shard_path.parent().unwrap()).await?;
        async_fs::rename(&tmppath, &shard_path).await?;

        // Shard created successfully!
        Ok(())
    }

    async fn insert_shard_abort<'a>(
        &'a self,
        mut insertion_shard: InsertionShard<'a, N>,
    ) -> Result<(), IOError> {
        // By construction, the [`InsertionShard`] may only call this method on
        // its own instance of the [`ShardStore`]
        assert!(self as *const _ == insertion_shard.store as *const _);

        // We consume and then immediately drop the struct, and never set
        // `InsertionShard.inner` to `None` otherwise, so this must succeed:
        let (tmppath, mut tmpfile) = insertion_shard.inner.take().unwrap();
        std::mem::drop(insertion_shard);

        // Flush and close the temporary file:
        tmpfile.flush().await?;
        std::mem::drop(tmpfile);

        // Remove the temporary file:
        async_fs::remove_file(&tmppath).await?;

        Ok(())
    }

    pub async fn get_shard<'a>(&'a self, digest: &[u8; N]) -> Result<Shard<'a, N>, IOError> {
        // Calculate the expected shard path based on the passed digest:
        let shard_path = self.digest_path_gen.get_path(&self.base_path, digest);

        // Try to open the file in read-only mode:
        let file = async_fs::File::open(&shard_path).await?;

        // If opening the file succeeded, hand out a [`Shard`] instance:
        Ok(Shard {
            store: self,
            inner: Some(file),
        })
    }

    fn get_shard_close(&self, _shard_file: async_fs::File) {
        // Close the file by dropping it. Flush not required, given it was
        // opened read-only.
    }

    pub fn iter_shards(&self) -> impl futures::stream::Stream<Item = [u8; N]> + '_ {
        use futures::stream::StreamExt;

        async_walkdir::WalkDir::new(&self.base_path)
            .map(|entry_res| entry_res.unwrap().path())
            .filter(|entry_path| {
                let prefix_free_entry_path = entry_path.strip_prefix(&self.base_path).unwrap();
                futures::future::ready(
                    !prefix_free_entry_path.starts_with("lock")
                        && !prefix_free_entry_path.starts_with("temp"),
                )
            })
            .filter(|entry_path| {
                futures::future::ready(entry_path.file_name().unwrap().len() == N * 2)
            })
            .map(|entry_path| {
                let mut digest = [0_u8; N];
                hex::decode_to_slice(
                    OsStrExt::as_bytes(entry_path.file_name().unwrap()),
                    &mut digest,
                )
                .unwrap();
                digest
            })
    }

    pub fn path(&self) -> PathBuf {
        self.base_path.clone()
    }
}

impl<const N: usize> Drop for ShardStore<N> {
    fn drop(&mut self) {
        // TODO: check if lock file still contains our current LockIdentity and,
        // in this case, remove it.
    }
}
