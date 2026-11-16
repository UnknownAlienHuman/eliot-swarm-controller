/// Owned temp-directory fixture: holds one `temp_dir()` subdirectory path
/// and removes it on drop, so a failed assertion never leaks the fixture
/// into the shared temp directory. The guard never creates: each test keeps
/// its own `create_dir`/`create_dir_all` setup (including `create_dir`,
/// which must observe a missing directory).
pub(crate) struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    pub(crate) fn own(path: std::path::PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
