use std::{
    fs::{self, OpenOptions},
    io::{self, Read},
    path::Path,
};

pub(crate) enum BoundedFileError {
    Io(io::Error),
    TooLarge,
}

impl From<io::Error> for BoundedFileError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub(crate) fn read_regular_file(
    path: &Path,
    max_bytes: usize,
) -> Result<Vec<u8>, BoundedFileError> {
    // Check the path before open so obvious nonregular inputs never reach it.
    // The file may change between these calls, so verify the opened handle as well.
    if !fs::metadata(path)?.is_file() {
        return Err(nonregular_file().into());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;

        // Opening a FIFO substituted after the path check must not wait for a writer.
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    #[cfg(windows)]
    #[allow(unsafe_code)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, GetFileType};

        // SAFETY: the live File owns the borrowed handle throughout this call.
        if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
            return Err(nonregular_file().into());
        }
    }
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(nonregular_file().into());
    }
    if metadata.len() > max_bytes as u64 {
        return Err(BoundedFileError::TooLarge);
    }
    read_capped(file, max_bytes)
}

fn nonregular_file() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "expected a regular file")
}

fn read_capped(reader: impl Read, max_bytes: usize) -> Result<Vec<u8>, BoundedFileError> {
    let mut bytes = Vec::new();
    // Keep the read bounded even if the opened file grows after the metadata check.
    reader
        .take((max_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(BoundedFileError::TooLarge);
    }
    Ok(bytes)
}
