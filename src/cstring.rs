#![allow(dead_code)]

use core::ffi::CStr;
use core::ffi::c_char;
use core::fmt;
use core::mem;
use core::str::FromStr;
use std::convert::Infallible;
use std::ptr::NonNull;

/// A null-terminated C string whose memory representation is exactly one
/// pointer — the same as `char*` in C.
///
/// Allocation uses the Rust global allocator. `Drop` recomputes the allocation
/// size by walking the null terminator, so no inline bookkeeping is needed.
#[repr(transparent)]
pub struct CString {
    buf: NonNull<c_char>,
}

unsafe impl Send for CString {}
unsafe impl Sync for CString {}

impl CString {
    pub fn as_ptr(&mut self) -> NonNull<c_char> {
        self.buf
    }

    pub fn as_raw(&self) -> *const c_char {
        self.buf.as_ptr()
    }

    pub fn as_c_str(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.buf.as_ptr()) }
    }

    pub fn to_bytes(&self) -> &[u8] {
        self.as_c_str().to_bytes()
    }

    pub fn into_ptr(self) -> NonNull<c_char> {
        let ptr = self.buf;
        mem::forget(self);
        ptr
    }

    pub fn into_raw(self) -> *mut c_char {
        self.into_ptr().as_ptr()
    }

    /// Zero-cost reinterpret of `Vec<CString>` into `Vec<*mut c_char>`.
    ///
    /// Each `CString` is leaked — `Drop` does not run. Use this when handing
    /// memory off to a C API like `execvp`.
    pub fn into_raw_vec(v: Vec<CString>) -> Vec<NonNull<c_char>> {
        let (ptr, len, cap) = v.into_raw_parts();
        unsafe { Vec::from_raw_parts(ptr as *mut NonNull<c_char>, len, cap) }
    }

    pub fn into_vec_of_options(v: Vec<CString>) -> Vec<Option<CString>> {
        unsafe { core::mem::transmute::<_, _>(v) }
    }
}

impl From<&[u8]> for CString {
    fn from(s: &[u8]) -> Self {
        let len = s.len() + 1;
        let layout = std::alloc::Layout::array::<u8>(len).unwrap();
        let alloc = unsafe { std::alloc::alloc(layout) };
        let buf = NonNull::new(alloc).expect("CString allocation failed");
        unsafe {
            std::ptr::copy_nonoverlapping(s.as_ptr(), buf.as_ptr(), s.len());
            *buf.add(s.len()).as_ptr() = 0;
        }
        Self { buf: buf.cast() }
    }
}

impl FromStr for CString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Infallible> {
        Ok(Self::from(s.as_bytes()))
    }
}

impl From<&CStr> for CString {
    fn from(s: &CStr) -> Self {
        Self::from(s.to_bytes())
    }
}

impl Drop for CString {
    fn drop(&mut self) {
        let len = unsafe { CStr::from_ptr(self.buf.as_ptr()).to_bytes().len() + 1 };
        let layout = std::alloc::Layout::array::<u8>(len).unwrap();
        unsafe {
            std::alloc::dealloc(self.buf.as_ptr().cast(), layout);
        }
    }
}

impl Clone for CString {
    fn clone(&self) -> Self {
        Self::from_str(unsafe { std::str::from_utf8_unchecked(self.to_bytes()) }).unwrap()
    }
}

impl fmt::Debug for CString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use core::mem;

    use super::*;

    const _: () = assert!(mem::size_of::<CString>() == mem::size_of::<*mut c_char>());
    const _: () = assert!(mem::size_of::<Option<CString>>() == mem::size_of::<*mut c_char>());

    #[test]
    fn roundtrip() {
        let s = "hello world";
        let c = CString::from_str(s).unwrap();
        assert_eq!(c.to_bytes(), b"hello world");
    }

    #[test]
    fn null_terminated() {
        let s = "test";
        let c = CString::from_str(s).unwrap();
        let bytes = c.to_bytes();
        let after = unsafe { *c.as_raw().add(bytes.len()) };
        assert_eq!(after, 0);
    }

    #[test]
    fn empty_string() {
        let c = CString::from_str("").unwrap();
        assert_eq!(c.to_bytes(), b"");
        let after = unsafe { *c.as_raw() };
        assert_eq!(after, 0);
    }

    #[test]
    fn clone_is_equal() {
        let c = CString::from_str("clone me").unwrap();
        let d = c.clone();
        assert_eq!(c.to_bytes(), d.to_bytes());
    }

    #[test]
    fn drop_does_not_crash() {
        let c = CString::from_str("drop me").unwrap();
        drop(c);
    }

    #[test]
    fn into_raw_returns_valid_ptr() {
        let c = CString::from_str("leaked").unwrap();
        let ptr = c.into_raw();
        let bytes = unsafe { CStr::from_ptr(ptr).to_bytes() };
        assert_eq!(bytes, b"leaked");
    }

    #[test]
    fn into_raw_vec_reinterprets() {
        let v = vec![
            CString::from_str("a").unwrap(),
            CString::from_str("b").unwrap(),
        ];
        let ptrs = CString::into_raw_vec(v);
        assert_eq!(ptrs.len(), 2);
        let s0 = unsafe { CStr::from_ptr(ptrs[0].as_ptr()).to_str().unwrap() };
        assert_eq!(s0, "a");
        let s1 = unsafe { CStr::from_ptr(ptrs[1].as_ptr()).to_str().unwrap() };
        assert_eq!(s1, "b");
    }
}
