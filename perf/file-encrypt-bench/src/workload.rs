//! Test-file generation and OS page-cache control.
//!
//! Files are drawn from a **log-uniform** size distribution: lots of small
//! files, a few big ones — the realistic shape of a real directory. The sizes
//! are scattered across the index order (not sorted), so the feeder hands a
//! mix of small and large files to the workers rather than clustering all the
//! large files at the end. That is what exercises work-stealing under skew.
//!
//! **File content is deliberately compressible**: each file is filled from a
//! per-file-seeded LCG over a small (16-symbol) alphabet. That keeps the data
//! low-entropy (so zstd does real optimal-parsing work — the heavy CPU stage)
//! while remaining non-periodic within a file (so the compressor can't just
//! latch onto a single repeating block and finish instantly). Size skew then
//! drives *both* IO and CPU skew — the regime the pipeline is built for.
//! (A tinier alphabet makes zstd's match-finder explode and run pathologically
//! slowly; 16 symbols is a sane, realistic-ish compressible payload.)
//!
//! All three engines read the same inputs; to keep the comparison honest each
//! engine should start against a cold read cache. On Linux we evict the cached
//! pages (`POSIX_FADV_DONTNEED`) before every engine run.

use std::{
    io::Write,
    path::{Path, PathBuf},
};

/// One generated input file: its path and byte size.
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
}

/// Deterministic pseudo-random in `[0,1)` from an integer index — a hashed
/// scatter so that size-by-index isn't monotonic (no "all big files at the
/// tail"). No external RNG dependency, and reproducible across runs.
fn hash01(i: usize) -> f64 {
    let mut x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 29;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 32;
    (x as f64) / (u64::MAX as f64)
}

/// Generate `count` files under `dir` with log-uniform sizes in
/// `[min_bytes, max_bytes]`, sizes scattered across indices. Returns the list
/// sorted by name (deterministic) plus the total bytes written.
///
/// Each file's content is generated on the fly by a per-file LCG over an
/// 8-symbol alphabet (compressible, non-periodic). See the module docs for why.
pub fn generate(
    dir: &Path,
    count: usize,
    min_bytes: u64,
    max_bytes: u64,
) -> std::io::Result<(Vec<FileEntry>, u64)> {
    std::fs::create_dir_all(dir)?;
    // Wipe any leftovers from a previous run.
    clean_dir(dir)?;

    let log_min = (min_bytes as f64).ln();
    let log_max = (max_bytes as f64).ln();

    let mut entries = Vec::with_capacity(count);
    let mut total: u64 = 0;
    for i in 0..count {
        let u = hash01(i);
        let size = (log_min + (log_max - log_min) * u).exp().round() as u64;
        let size = size.clamp(min_bytes, max_bytes);
        let name = format!("file_{i:04}.bin");
        let path = dir.join(name);
        write_file(&path, size as usize, i as u64)?;
        entries.push(FileEntry { path, size });
        total += size;
    }
    // Deterministic order (by name = by index). Engines iterate this list.
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok((entries, total))
}

/// Per-file LCG state. Same constants as SplitMix64 — full-period 64-bit,
/// cheap, good statistical quality for this purpose.
fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn write_file(path: &Path, size: usize, seed: u64) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut buf = vec![0u8; 1 << 16]; // 64 KiB chunks
    let mut written = 0;
    while written < size {
        let n = buf.len().min(size - written);
        for b in &mut buf[..n] {
            // 16-symbol alphabet (b'a'..b'p') → ~4 bits/byte entropy:
            // compressible, but the LCG stream is non-periodic so the
            // compressor does real entropy-coding + optimal-parsing work
            // instead of latching onto a single repeating block.
            *b = b'a' + ((lcg_next(&mut state) >> 33) as u8 & 0x0F);
        }
        f.write_all(&buf[..n])?;
        written += n;
    }
    // Persist to disk so the pages are clean (evictable) in the page cache.
    f.sync_all()?;
    Ok(())
}

/// Write one engine output file. When `fsync` is set, the file is flushed to
/// disk (`sync_all`) — modelling a durable "write back". Without it the write
/// is buffered (returns once in the page cache), which is fast but means the
/// benchmark is effectively memory-bound rather than disk-bound.
pub fn write_output(path: &Path, data: &[u8], fsync: bool) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    f.write_all(data)?;
    if fsync {
        f.sync_all()?;
    }
    Ok(())
}

/// Remove a directory's contents but keep the directory itself.
pub fn clean_dir(dir: &Path) -> std::io::Result<()> {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}

/// Evict a directory's files from the OS page cache (Linux only).
///
/// `POSIX_FADV_DONTNEED` discards clean cached pages so the next read goes to
/// disk — keeping the read side of each engine run comparably cold. On other
/// platforms this is a no-op; cold-cache fairness then relies on total data
/// size exceeding cache.
pub fn evict_dir_cache(dir: &Path) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in rd.flatten() {
            if let Ok(f) = std::fs::File::open(entry.path()) {
                // Safety: posix_fadvise is a no-op syscall on a valid fd; the
                // fd is closed immediately after. Ignored return — best-effort.
                unsafe {
                    libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
                }
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = dir;
    }
}

/// Human-readable byte size (binary). Used by the report printer.
pub fn fmt_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < UNITS.len() {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {u}", u = UNITS[i])
}

/// Filesystem identification for the data directory.
#[cfg(target_os = "linux")]
pub fn fs_type_name(path: &Path) -> String {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let c = match CString::new(path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => return "<bad path>".into(),
    };
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut buf) } != 0 {
        return "<statfs failed>".into();
    }
    // `f_type`'s integer signedness differs across libc targets; cast the
    // constants with `as _` so this compiles warning-free on both.
    let ft = buf.f_type;
    if ft == 0x0102_1994_u32 as _ {
        "tmpfs"
    } else if ft == 0x8584_5868_u32 as _ {
        "ramfs"
    } else if ft == 0x9123_683E_u32 as _ {
        "btrfs"
    } else if ft == 0xEF53_u32 as _ {
        "ext4"
    } else if ft == 0x5846_5342_u32 as _ {
        "xfs"
    } else if ft == 0x794c_7630_u32 as _ {
        "overlay"
    } else if ft == 0x6573_5546_u32 as _ {
        "fuse"
    } else {
        "other"
    }
    .into()
}

#[cfg(not(target_os = "linux"))]
pub fn fs_type_name(_path: &Path) -> String {
    "unknown".into()
}

/// True when the path is backed by RAM (tmpfs/ramfs) — the regime where the
/// benchmark's IO overlap advantage does not apply (there is no blocking disk
/// IO to overlap with CPU).
#[cfg(target_os = "linux")]
pub fn is_ram_backed(path: &Path) -> bool {
    matches!(fs_type_name(path).as_str(), "tmpfs" | "ramfs")
}

#[cfg(not(target_os = "linux"))]
pub fn is_ram_backed(_path: &Path) -> bool {
    false
}
