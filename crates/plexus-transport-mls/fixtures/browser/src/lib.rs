//! The browser binding of the wire profile.
//!
//! A WebAssembly module with no imports and four exports. The host copies a
//! record's bytes into memory it asked for, calls [`pmls_observe`], and reads
//! back one line of UTF-8 (`plexus_transport_mls::wire::conformance::line`).
//! `web/wire.mjs` wraps it.
//!
//! The decoding is the wire crate's, the same source the native endpoint
//! compiles; what this fixture shows is that it means the same on a 32-bit
//! target behind a byte-copying boundary.

use plexus_transport_mls::wire::conformance::{line, observe, Record};

/// The line for `bytes` read as the record numbered `record`.
#[must_use]
pub fn observe_line(record: u32, bytes: &[u8]) -> String {
    match Record::from_code(record) {
        Some(record) => line(&observe(record, bytes)),
        None => String::from("error\tbinding.unknown_record"),
    }
}

/// Version of this ABI.
#[no_mangle]
pub extern "C" fn pmls_abi_version() -> u32 {
    1
}

/// Reserves `len` bytes and returns their address. Free with [`pmls_free`].
#[no_mangle]
pub extern "C" fn pmls_alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(len.max(1));
    let ptr = buf.as_mut_ptr();
    core::mem::forget(buf);
    ptr
}

/// Releases a reservation of `len` bytes made by [`pmls_alloc`], or a result
/// of [`pmls_observe`] with its length.
///
/// # Safety
/// `ptr` and `len` are those of one reservation that has not been released.
#[no_mangle]
pub unsafe extern "C" fn pmls_free(ptr: *mut u8, len: usize) {
    // SAFETY: by the contract above this is the allocation `pmls_alloc` or
    // `pmls_observe` leaked, with the capacity it was made with.
    drop(unsafe { Vec::from_raw_parts(ptr, 0, len.max(1)) });
}

/// Observes the `len` bytes at `ptr` as the record numbered `record`.
///
/// Returns the address of the line in the high 32 bits and its length in the
/// low 32 bits. The caller releases it with [`pmls_free`].
///
/// # Safety
/// `ptr` is a reservation of at least `len` bytes, all written.
#[no_mangle]
pub unsafe extern "C" fn pmls_observe(record: u32, ptr: *const u8, len: usize) -> u64 {
    // SAFETY: by the contract above the range is initialized and stays
    // reserved for the call.
    let bytes = unsafe { core::slice::from_raw_parts(ptr, len) };
    let mut out = observe_line(record, bytes).into_bytes();
    out.shrink_to_fit();
    // An exact capacity, so `pmls_free(ptr, len)` rebuilds this allocation.
    let mut out = out.into_boxed_slice();
    let (ptr, len) = (out.as_mut_ptr(), out.len());
    core::mem::forget(out);
    ((ptr as usize as u64) << 32) | len as u64
}
