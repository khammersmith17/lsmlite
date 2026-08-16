pub mod worker;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/*
* Conditions that define a valid restart:
*   Any number of SSTable files and/or WAL files may be present on disk.
*
*   SSTables are sorted newest-first by timestamp (from filename) as this is the
*   order they are loaded into the SSTable cache for reads.
*
*   WAL files are sorted oldest-first by timestamp. All WAL files except the
*   newest represent full memtables that were rotated but not yet flushed to
*   SSTables before shutdown. The newest WAL file represents the active memtable
*   at the time of shutdown, which may be partially filled.
*
*   The caller is responsible for deciding what to do with each set:
*     - Full WALs (all but last): replay and flush to SSTable.
*     - Active WAL (last): replay into the new active memtable.
* */

enum DatabaseFile {
    Wal,
    SSTable,
}

impl DatabaseFile {
    fn from_path(path: &Path) -> Option<DatabaseFile> {
        let ext_bytes = path.extension()?.as_encoded_bytes();
        match ext_bytes {
            b"sstable" => Some(DatabaseFile::SSTable),
            b"log"
                if path
                    .file_name()
                    .expect("Unable to retrieve filename on file type parsing")
                    .to_str()
                    .expect("Unable to get string out of filename on file type parsing")
                    .starts_with("wal") =>
            {
                Some(DatabaseFile::Wal)
            }
            _ => None,
        }
    }
}

pub struct WalArtifact {
    pub filename: PathBuf,
    ts: u128,
}

impl WalArtifact {
    fn new(filename: PathBuf) -> WalArtifact {
        let name = filename.file_name().unwrap().to_str().unwrap();
        // "wal.{ts}.log" → nth(1) is the timestamp
        let ts: u128 = name
            .split('.')
            .nth(1)
            .expect("Invalid WAL filename")
            .parse()
            .expect("Invalid WAL timestamp");
        WalArtifact { filename, ts }
    }
}

impl PartialEq for WalArtifact {
    fn eq(&self, other: &Self) -> bool {
        self.filename == other.filename
    }
}

impl Eq for WalArtifact {}

impl PartialOrd for WalArtifact {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ts.cmp(&other.ts))
    }
}

impl Ord for WalArtifact {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct SSTableArtifact {
    pub filename: PathBuf,
    ts: u128,
}

impl SSTableArtifact {
    fn new(filename: PathBuf) -> SSTableArtifact {
        let name = filename.file_name().unwrap().to_str().unwrap();
        // "{ts}.sstable" → next() is the timestamp
        let ts: u128 = name
            .split('.')
            .next()
            .expect("Invalid SSTable filename")
            .parse()
            .expect("Invalid SSTable timestamp");
        SSTableArtifact { filename, ts }
    }
}

impl PartialEq for SSTableArtifact {
    fn eq(&self, other: &Self) -> bool {
        self.filename == other.filename
    }
}

impl Eq for SSTableArtifact {}

impl PartialOrd for SSTableArtifact {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.ts.cmp(&other.ts))
    }
}

impl Ord for SSTableArtifact {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub(crate) fn scan_directory(dir: &Path) -> (Vec<SSTableArtifact>, Vec<WalArtifact>) {
    let mut wal_files: Vec<WalArtifact> = Vec::new();
    let mut sstable_files: Vec<SSTableArtifact> = Vec::new();

    for entry in WalkDir::new(dir).max_depth(1).into_iter() {
        let entry = entry.expect("Unable to acquire entry");
        let path = entry.path();

        let Some(filetype) = DatabaseFile::from_path(path) else {
            continue;
        };
        match filetype {
            DatabaseFile::Wal => {
                wal_files.push(WalArtifact::new(path.to_path_buf()));
            }
            DatabaseFile::SSTable => {
                sstable_files.push(SSTableArtifact::new(path.to_path_buf()));
            }
        }
    }

    // Sort SSTables from newest to oldestk (reverse Ord), as this is how they will be read into SSTableCache.
    sstable_files.sort_by(|a, b| b.cmp(a));
    wal_files.sort();

    (sstable_files, wal_files)
}

pub fn get_restart_state() -> (Vec<SSTableArtifact>, Vec<WalArtifact>) {
    scan_directory(Path::new("."))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> TempDir {
            let id = DIR_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let path = PathBuf::from(format!("test_restart_dir_{id}"));
            fs::create_dir_all(&path).unwrap();
            TempDir(path)
        }

        fn create(&self, name: &str) {
            fs::File::create(self.0.join(name)).unwrap();
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn test_empty_directory_returns_no_files() {
        let dir = TempDir::new();
        let (sstables, wals) = scan_directory(&dir.0);
        drop(dir);
        assert!(sstables.is_empty());
        assert!(wals.is_empty());
    }

    #[test]
    fn test_sstable_files_detected() {
        let dir = TempDir::new();
        dir.create("1000.sstable");
        dir.create("2000.sstable");
        let (sstables, wals) = scan_directory(&dir.0);
        drop(dir);
        assert_eq!(sstables.len(), 2);
        assert!(wals.is_empty());
    }

    #[test]
    fn test_sstables_sorted_newest_first() {
        let dir = TempDir::new();
        dir.create("1000.sstable");
        dir.create("3000.sstable");
        dir.create("2000.sstable");
        let (sstables, _) = scan_directory(&dir.0);
        drop(dir);
        assert_eq!(sstables[0].filename.file_name().unwrap(), "3000.sstable");
        assert_eq!(sstables[1].filename.file_name().unwrap(), "2000.sstable");
        assert_eq!(sstables[2].filename.file_name().unwrap(), "1000.sstable");
    }

    #[test]
    fn test_wal_file_detected() {
        let dir = TempDir::new();
        dir.create("wal.1000.log");
        let (_, wals) = scan_directory(&dir.0);
        drop(dir);
        assert_eq!(wals.len(), 1);
        assert_eq!(wals[0].filename.file_name().unwrap(), "wal.1000.log");
    }

    #[test]
    fn test_multiple_wals_sorted_oldest_first() {
        let dir = TempDir::new();
        dir.create("wal.3000.log");
        dir.create("wal.1000.log");
        dir.create("wal.2000.log");
        let (_, wals) = scan_directory(&dir.0);
        drop(dir);
        assert_eq!(wals.len(), 3);
        assert_eq!(wals[0].filename.file_name().unwrap(), "wal.1000.log");
        assert_eq!(wals[1].filename.file_name().unwrap(), "wal.2000.log");
        assert_eq!(wals[2].filename.file_name().unwrap(), "wal.3000.log");
    }

    #[test]
    fn test_non_database_files_ignored() {
        let dir = TempDir::new();
        dir.create("notes.txt");
        dir.create("config.json");
        let (sstables, wals) = scan_directory(&dir.0);
        drop(dir);
        assert!(sstables.is_empty());
        assert!(wals.is_empty());
    }

    #[test]
    fn test_non_wal_log_files_ignored() {
        let dir = TempDir::new();
        dir.create("other.log");
        let (sstables, wals) = scan_directory(&dir.0);
        drop(dir);
        assert!(sstables.is_empty());
        assert!(wals.is_empty());
    }

    #[test]
    fn test_both_wal_and_sstable_detected() {
        let dir = TempDir::new();
        dir.create("1000.sstable");
        dir.create("wal.2000.log");
        let (sstables, wals) = scan_directory(&dir.0);
        drop(dir);
        assert_eq!(sstables.len(), 1);
        assert_eq!(wals.len(), 1);
    }
}
