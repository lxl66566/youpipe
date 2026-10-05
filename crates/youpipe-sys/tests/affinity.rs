//! `pin_current_thread_to` must return `false` for CPU ids at or beyond
//! `CPU_SETSIZE` (1024) instead of panicking: `libc::CPU_SET` indexes the
//! fixed-size word array inside `cpu_set_t`, and that bounds-check panic
//! aborts the process without unwinding.

use youpipe_sys::pin_current_thread_to;

#[test]
fn pin_rejects_cpu_ids_beyond_cpu_setsize() {
    assert!(!pin_current_thread_to(1024));
    assert!(!pin_current_thread_to(2000));
}
