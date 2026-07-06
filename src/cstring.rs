#![allow(dead_code)]

use core::convert::Infallible;
use core::ffi::c_char;
use core::fmt;
use core::marker::PhantomData;
use core::mem;
use core::ptr::NonNull;
use core::str::FromStr;

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
    pub fn try_from_bytes(s: &[u8]) -> Result<Self, CStringError> {
        if let Some(pos) = s.iter().position(|&b| b == 0) {
            return Err(CStringError::ContainsNull(pos));
        }
        // SAFETY: we just verified there are no interior null bytes.
        Ok(unsafe { from_bytes_unchecked(s) })
    }

    pub fn as_ptr(&mut self) -> NonNull<c_char> {
        self.buf
    }

    pub fn as_raw(&self) -> *const c_char {
        self.buf.as_ptr()
    }

    pub fn borrow(&self) -> CStr<'_> {
        CStr {
            buf: self.buf,
            _borrow: PhantomData,
        }
    }

    pub fn as_std_c_str(&self) -> &core::ffi::CStr {
        unsafe { core::ffi::CStr::from_ptr(self.as_raw()) }
    }

    pub fn to_bytes(&self) -> &[u8] {
        self.as_std_c_str().to_bytes()
    }

    pub fn into_ptr(self) -> NonNull<c_char> {
        let ptr = self.buf;
        mem::forget(self);
        ptr
    }

    pub fn into_raw(self) -> *mut c_char {
        self.into_ptr().as_ptr()
    }

    pub fn into_raw_option(this: Option<Self>) -> *mut c_char {
        match this {
            Some(s) => s.into_raw(),
            None => std::ptr::null_mut(),
        }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CStringError {
    /// Input contains a null byte at the given position.
    ContainsNull(usize),
}

impl fmt::Display for CStringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContainsNull(pos) => write!(f, "null byte at position {pos}"),
        }
    }
}

impl std::error::Error for CStringError {}

/// # Safety
///
/// `s` must not contain interior null bytes. Callers that cannot guarantee
/// this should use `TryFrom<&[u8]>` instead.
unsafe fn from_bytes_unchecked(s: &[u8]) -> CString {
    let len = s.len() + 1;
    let layout = std::alloc::Layout::array::<u8>(len).unwrap();
    let alloc = unsafe { std::alloc::alloc(layout) };
    let buf = NonNull::new(alloc).expect("CString allocation failed");
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), buf.as_ptr(), s.len());
        *buf.add(s.len()).as_ptr() = 0;
    }
    CString { buf: buf.cast() }
}

impl FromStr for CString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Infallible> {
        Ok(Self::from(s))
    }
}

impl<T: AsRef<str>> From<T> for CString {
    fn from(s: T) -> Self {
        // SAFETY: &str is guaranteed to not contain interior null bytes.
        unsafe { from_bytes_unchecked(s.as_ref().as_bytes()) }
    }
}

impl Drop for CString {
    fn drop(&mut self) {
        let len = self.as_std_c_str().to_bytes().len() + 1;
        let layout = std::alloc::Layout::array::<u8>(len).unwrap();
        unsafe {
            std::alloc::dealloc(self.buf.as_ptr().cast(), layout);
        }
    }
}

impl PartialEq for CString {
    fn eq(&self, other: &Self) -> bool {
        self.borrow() == other.borrow()
    }
}

impl Eq for CString {}

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

/// A borrowed C string reference. Only constructible via `CString::as_c_str()`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct CStr<'a> {
    buf: NonNull<c_char>,
    _borrow: PhantomData<&'a CString>,
}

unsafe impl Send for CStr<'_> {}
unsafe impl Sync for CStr<'_> {}

impl PartialEq for CStr<'_> {
    fn eq(&self, other: &Self) -> bool {
        let ptr_a = self.buf.as_ptr();
        let ptr_b = other.buf.as_ptr();

        let mut i = 0isize;
        loop {
            let a = unsafe { *ptr_a.offset(i) };
            let b = unsafe { *ptr_b.offset(i) };
            if a != b {
                return false;
            }
            if a == 0 {
                return true;
            }
            i += 1;
        }
    }
}

impl Eq for CStr<'_> {}

impl<'a> CStr<'a> {
    pub fn as_std(self) -> &'a core::ffi::CStr {
        unsafe { core::ffi::CStr::from_ptr(self.as_raw()) }
    }

    pub fn to_bytes(self) -> &'a [u8] {
        self.as_std().to_bytes()
    }

    pub fn as_raw(self) -> *const c_char {
        self.buf.as_ptr()
    }

    pub fn as_raw_option(this: Option<Self>) -> *const c_char {
        match this {
            Some(s) => s.as_raw(),
            None => std::ptr::null(),
        }
    }
}

impl<'a> From<&'a CString> for CStr<'a> {
    fn from(s: &'a CString) -> Self {
        s.borrow()
    }
}

#[cfg(test)]
mod tests {
    use core::mem;

    use super::*;

    const _: () = assert!(mem::size_of::<CString>() == mem::size_of::<*mut c_char>());
    const _: () = assert!(mem::size_of::<Option<CString>>() == mem::size_of::<*mut c_char>());
    const _: () = assert!(mem::size_of::<CStr>() == mem::size_of::<*mut c_char>());
    const _: () = assert!(mem::size_of::<Option<CStr>>() == mem::size_of::<*mut c_char>());

    #[test]
    fn cstr_borrow_from_cstring() {
        let c = CString::from_str("hello").unwrap();
        let borrowed = c.borrow();
        assert_eq!(borrowed.to_bytes(), b"hello");
        assert_eq!(borrowed.as_raw(), c.as_raw());
    }

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
        let bytes = unsafe { core::ffi::CStr::from_ptr(ptr).to_bytes() };
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
        let s0 = unsafe {
            core::ffi::CStr::from_ptr(ptrs[0].as_ptr())
                .to_str()
                .unwrap()
        };
        assert_eq!(s0, "a");
        let s1 = unsafe {
            core::ffi::CStr::from_ptr(ptrs[1].as_ptr())
                .to_str()
                .unwrap()
        };
        assert_eq!(s1, "b");
    }

    #[test]
    fn try_from_valid() {
        let c = CString::try_from_bytes(b"hello".as_slice()).unwrap();
        assert_eq!(c.to_bytes(), b"hello");
    }

    #[test]
    fn try_from_empty() {
        let c = CString::try_from_bytes(b"".as_slice()).unwrap();
        assert_eq!(c.to_bytes(), b"");
    }

    #[test]
    fn try_from_interior_null() {
        let err = CString::try_from_bytes(b"ab\0cd".as_slice()).unwrap_err();
        assert_eq!(err, CStringError::ContainsNull(2));
    }

    #[test]
    fn try_from_leading_null() {
        let err = CString::try_from_bytes(b"\0hello".as_slice()).unwrap_err();
        assert_eq!(err, CStringError::ContainsNull(0));
    }

    #[test]
    fn try_from_trailing_null() {
        let err = CString::try_from_bytes(b"hello\0".as_slice()).unwrap_err();
        assert_eq!(err, CStringError::ContainsNull(5));
    }

    #[test]
    fn try_from_display() {
        let err = CStringError::ContainsNull(3);
        assert_eq!(format!("{err}"), "null byte at position 3");
    }
}
