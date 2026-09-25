//! CPU-affinity shims for optional pool-worker pinning.
//!
//! Miri-gated out (`--cfg miri`): `sched_setaffinity` is an unsupported
//! syscall there, and the pin path is opt-in at runtime (default off), so a
//! miri run never reaches it anyway — the gate just keeps the contract
//! explicit.

#[cfg(all(target_os = "linux", not(miri)))]
mod imp {
    use std::{mem::size_of, ptr};

    use libc::{cpu_set_t, sched_getaffinity, sched_setaffinity};

    /// CPUs the calling thread is allowed to run on, ascending. Empty on
    /// failure — callers treat "no set" as "don't pin". `cpu_set_t` covers
    /// `CPU_SETSIZE` (1024) CPUs; machines whose whole allowed set lives
    /// beyond that are not supported (none exist in practice).
    #[must_use]
    pub fn allowed_cpus() -> Vec<u32> {
        // SAFETY: `set` is a zeroed kernel-out struct of the exact expected
        // size; pid 0 means the calling thread.
        unsafe {
            let mut set = std::mem::zeroed::<cpu_set_t>();
            if sched_getaffinity(0, size_of::<cpu_set_t>(), ptr::from_mut(&mut set)) != 0 {
                return Vec::new();
            }
            (0..libc::CPU_SETSIZE as u32)
                .filter(|&i| libc::CPU_ISSET(i as usize, &set))
                .collect()
        }
    }

    /// Pin the calling thread to exactly `cpu`. Returns `false` on failure
    /// (invalid CPU, permissions) — callers proceed unpinned.
    #[must_use]
    pub fn pin_current_thread_to(cpu: u32) -> bool {
        // SAFETY: same as `allowed_cpus`; the set holds exactly one bit.
        unsafe {
            let mut set = std::mem::zeroed::<cpu_set_t>();
            libc::CPU_SET(cpu as usize, &mut set);
            sched_setaffinity(0, size_of::<cpu_set_t>(), ptr::from_ref(&set)) == 0
        }
    }
}

#[cfg(any(not(target_os = "linux"), miri))]
mod imp {
    /// Non-Linux fallback: no affinity support, never pin.
    #[must_use]
    pub fn allowed_cpus() -> Vec<u32> {
        Vec::new()
    }

    #[must_use]
    pub fn pin_current_thread_to(_cpu: u32) -> bool {
        false
    }
}

pub use imp::{allowed_cpus, pin_current_thread_to};
