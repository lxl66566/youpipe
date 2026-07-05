//! File encryption schemes shared by all three engines.
//!
//! Two selectable via [`Task`] (set by `FC_TASK` in `main`):
//!
//! - [`Task::Aes`] — plain AES-256-GCM. Fast (AES-NI), so the CPU stage is
//!   marginal and the workload is IO/memory bound. This is the original light
//!   variant.
//! - [`Task::CompressEncrypt`] — **zstd compress then AES-256-GCM encrypt**.
//!   This is the real backup-encryption pipeline (restic / borg / age-with-zstd
//!   all do compress-then-encrypt). zstd at a high level is genuinely CPU-heavy
//!   and its cost scales with input size — exactly the regime where youpipe's
//!   read/CPU/write pipelining pays off (the CPU stage has real work to overlap
//!   with the blocking `fsync` of other files).
//!
//! Wire format of one sealed blob is unchanged for `Aes` (`nonce ‖ ct ‖ tag`,
//! +28 B, ct == plaintext). For `CompressEncrypt` it is
//! `nonce ‖ E_zstd(plaintext) ‖ tag`, so the output size varies with
//! compressibility (smaller than input for compressible data).
//!
//! All three engines call [`Cipher::seal`] — the per-file work is identical
//! across them, so timing differences reflect only scheduling / IO topology.

use aes_gcm::{
    Aes256Gcm,
    aead::{AeadInPlace, KeyInit, generic_array::GenericArray},
};

/// Random nonce length (AES-GCM standard = 96 bit).
pub const NONCE_LEN: usize = 12;
/// GCM authentication tag length.
pub const TAG_LEN: usize = 16;
/// Per-file crypto overhead: nonce + tag (the zstd stage adds none of its own
/// framing on top of `encode_all`'s output beyond the compressed bytes).
pub const OVERHEAD: usize = NONCE_LEN + TAG_LEN;

/// Which per-file transform to benchmark.
#[derive(Clone, Copy)]
pub enum Task {
    /// AES-256-GCM only (light CPU).
    Aes,
    /// zstd(level) then AES-256-GCM (heavy, size-proportional CPU).
    CompressEncrypt(i32),
}

impl Task {
    pub fn label(self) -> &'static str {
        match self {
            Task::Aes => "AES-256-GCM",
            Task::CompressEncrypt(_) => "zstd + AES-256-GCM (compress-then-encrypt)",
        }
    }
}

/// AES-256-GCM state plus the selected per-file task. Cheap to share behind an
/// `Arc`; `seal`/`open` take `&self` so the key schedule is paid once.
pub struct Cipher {
    aes: Aes256Gcm,
    task: Task,
}

impl Cipher {
    pub fn new(task: Task) -> Self {
        let mut key = [0u8; 32];
        getrandom::getrandom(&mut key).expect("getrandom key");
        Self {
            aes: Aes256Gcm::new(GenericArray::from_slice(&key)),
            task,
        }
    }

    /// Seal `plaintext` per the selected [`Task`].
    pub fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
        match self.task {
            Task::Aes => self.aes_seal(plaintext),
            Task::CompressEncrypt(level) => {
                // Compress first (the heavy CPU stage), then encrypt the
                // compressed bytes. `encode_all` owns its allocations.
                let compressed = zstd::encode_all(plaintext, level).expect("zstd compress");
                self.aes_seal(&compressed)
            }
        }
    }

    /// Inverse of [`Self::seal`]. Used by the post-run verifier.
    pub fn open(&self, blob: &[u8]) -> Vec<u8> {
        let decrypted = self.aes_open(blob);
        match self.task {
            Task::Aes => decrypted,
            Task::CompressEncrypt(_) => zstd::decode_all(&decrypted[..]).expect("zstd decompress"),
        }
    }

    /// AES-256-GCM seal of `data` → `nonce ‖ ciphertext ‖ tag` (one alloc).
    fn aes_seal(&self, data: &[u8]) -> Vec<u8> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        getrandom::getrandom(&mut nonce_bytes).expect("getrandom nonce");
        let nonce = GenericArray::from_slice(&nonce_bytes);

        let mut out = Vec::with_capacity(NONCE_LEN + data.len() + TAG_LEN);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(data);
        // CTR-XOR the data slice in place (out[NONCE_LEN..]) and return the
        // GHASH tag separately so the nonce prefix stays unencrypted.
        let tag = self
            .aes
            .encrypt_in_place_detached(nonce, b"", &mut out[NONCE_LEN..])
            .expect("aes-gcm encrypt");
        out.extend_from_slice(tag.as_slice());
        out
    }

    fn aes_open(&self, blob: &[u8]) -> Vec<u8> {
        assert!(blob.len() > OVERHEAD, "ciphertext too short for nonce+tag");
        let nonce = GenericArray::from_slice(&blob[..NONCE_LEN]);
        let ct_end = blob.len() - TAG_LEN;
        let mut pt = blob[NONCE_LEN..ct_end].to_vec();
        let tag = GenericArray::from_slice(&blob[ct_end..]);
        self.aes
            .decrypt_in_place_detached(nonce, b"", &mut pt, tag)
            .expect("aes-gcm decrypt");
        pt
    }
}
