extern crate alloc;

use core::alloc::Layout;
use core::ptr::NonNull;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::cell::Cell;
use std::num::NonZeroU32;

pub const ARENA_ALIGN: usize = 1 << 31;
pub const FOOTER_SIZE: u32 = size_of::<AllocFooter>() as u32;

#[derive(Clone, Copy)]
#[repr(transparent)]
struct AllocFooter(u32);

impl AllocFooter {
    const DEALLOCATED_BIT: u32 = 1 << 31;
    const SIZE_MASK: u32 = 0x7FFF_FFFF;

    fn new(size: u32, deallocated: bool) -> Self {
        Self(
            size | if deallocated {
                Self::DEALLOCATED_BIT
            } else {
                0
            },
        )
    }

    fn size(self) -> u32 {
        self.0 & Self::SIZE_MASK
    }

    fn is_deallocated(self) -> bool {
        self.0 & Self::DEALLOCATED_BIT != 0
    }

    fn mark_deallocated(&mut self) {
        self.0 |= Self::DEALLOCATED_BIT;
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ArenaAlloc {
    pub size: NonZeroU32,
    pub offset: u32,
}

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct ArenaAllocSlice(pub NonNull<[u8]>);

pub struct Arena {
    buf: NonNull<u8>, // page aligned
    offset: Cell<u32>,
    capacity: u32,
}

impl Arena {
    pub fn new(capacity: u32) -> Self {
        let layout = Layout::from_size_align(capacity as usize, ARENA_ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self {
            buf: unsafe { NonNull::new_unchecked(ptr) },
            offset: Cell::new(0),
            capacity,
        }
    }

    pub fn alloc(&self, layout: Layout) -> Option<ArenaAlloc> {
        let size = NonZeroU32::new(layout.size() as u32)?;

        let align = layout.align() as u32;
        let aligned = (self.offset.get() + align - 1) & !(align - 1);
        let padded = aligned + size.get();
        let total = padded.checked_add(FOOTER_SIZE)?;
        if total > self.capacity {
            return None;
        }
        let footer_addr = unsafe { self.buf.as_ptr().add(padded as usize).cast::<AllocFooter>() };
        unsafe {
            footer_addr.write_unaligned(AllocFooter::new(padded - self.offset.get(), false));
        }
        self.offset.set(padded + FOOTER_SIZE);
        Some(ArenaAlloc {
            offset: aligned,
            size,
        })
    }

    pub fn alloc_type<T>(&self) -> Option<ArenaAlloc> {
        self.alloc(Layout::new::<T>())
    }

    pub fn alloc_write<T>(&self, value: T) -> Option<ArenaAlloc> {
        let alloc = self.alloc_type::<T>()?;
        unsafe {
            core::ptr::write(self.ptr_for(&alloc) as *mut T, value);
        }
        Some(alloc)
    }

    pub fn ptr_for(&self, alloc: &ArenaAlloc) -> *mut u8 {
        unsafe { self.buf.as_ptr().add(alloc.offset as usize) }
    }

    pub fn get_alloc_slice(&self, alloc: &ArenaAlloc) -> ArenaAllocSlice {
        let ptr = unsafe { NonNull::new_unchecked(self.ptr_for(alloc)) };
        ArenaAllocSlice(NonNull::slice_from_raw_parts(
            ptr,
            alloc.size.get() as usize,
        ))
    }

    pub fn read<T>(&self, alloc: &ArenaAlloc) -> T {
        unsafe { core::ptr::read(self.ptr_for(alloc) as *const T) }
    }

    /// # Safety
    /// `alloc` must have been returned by a prior `alloc` call on this arena,
    /// and must not have been deallocated already.
    pub unsafe fn dealloc(&self, alloc: ArenaAlloc) {
        unsafe {
            let ptr = self.ptr_for(&alloc);
            let footer_addr = ptr.add(alloc.size.get() as usize).cast::<AllocFooter>();
            let mut val = footer_addr.read_unaligned();
            val.mark_deallocated();
            footer_addr.write_unaligned(val);

            while self.offset.get() > 0 {
                let top_addr = self
                    .buf
                    .as_ptr()
                    .add((self.offset.get() - FOOTER_SIZE) as usize)
                    .cast::<AllocFooter>();
                let top = top_addr.read_unaligned();
                if !top.is_deallocated() {
                    break;
                }
                self.offset
                    .set(self.offset.get() - FOOTER_SIZE - top.size());
            }
        }
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity as usize, ARENA_ALIGN).unwrap();
        unsafe { dealloc(self.buf.as_ptr(), layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_success() {
        let arena = Arena::new(4096);
        let layout = Layout::from_size_align(64, 1).unwrap();
        let alloc = arena.alloc(layout).expect("alloc should succeed");
        assert_eq!(alloc.offset, 0);
        assert_eq!(alloc.size.get(), 64);
    }

    #[test]
    fn alloc_writes_footer_and_advances_offset() {
        let arena = Arena::new(4096);
        let layout = Layout::from_size_align(32, 1).unwrap();
        let alloc = arena.alloc(layout).unwrap();

        // padded = aligned + size = 0 + 32 = 32
        // total = 32 + FOOTER_SIZE = 36
        let expected_total = 32 + FOOTER_SIZE;
        assert_eq!(arena.offset.get(), expected_total);
        let ptr = arena.ptr_for(&alloc);
        assert_eq!(alloc.offset, 0);

        // Manually read the footer at offset 32 (after payload)
        let footer_addr = unsafe { ptr.add(alloc.size.get() as usize).cast::<AllocFooter>() };
        let footer = unsafe { footer_addr.read_unaligned() };
        // footer stores padded - prev_offset = 32 - 0 = 32
        assert_eq!(footer.size(), 32);
        assert!(!footer.is_deallocated());
    }

    #[test]
    fn alloc_exhaustion() {
        let arena = Arena::new(100);
        // Alloc most of the capacity
        let layout = Layout::from_size_align(96, 1).unwrap();
        let alloc = arena.alloc(layout);
        assert!(alloc.is_some());
        // Next alloc should fail (96 + FOOTER_SIZE = 100, exactly at capacity,
        // but +FOOTER_SIZE for next overflows)
        let layout2 = Layout::from_size_align(4, 1).unwrap();
        let alloc2 = arena.alloc(layout2);
        assert!(alloc2.is_none());
    }

    #[test]
    fn alloc_type_and_read_roundtrip() {
        let arena = Arena::new(4096);
        let alloc = arena.alloc_write(42u64).expect("alloc_write failed");
        let value: u64 = arena.read(&alloc);
        assert_eq!(value, 42u64);
    }

    #[test]
    fn alloc_write_vec_roundtrip() {
        let arena = Arena::new(4096);
        let v = vec![1u8, 2, 3, 4, 5];
        let alloc = arena.alloc_write(v).expect("alloc_write Vec failed");
        // read back — this creates a bitwise copy of Vec (heap-allocated data is
        // shared)
        let v2: Vec<u8> = arena.read(&alloc);
        assert_eq!(v2, vec![1, 2, 3, 4, 5]);
        // forget v2 to avoid double-free (the arena still holds the bits)
        core::mem::forget(v2);
    }

    #[test]
    fn ptr_for_is_within_bounds() {
        let arena = Arena::new(4096);
        let alloc = arena.alloc_write([0u8; 16]).unwrap();
        let ptr = arena.ptr_for(&alloc);
        let start = arena.buf.as_ptr() as usize;
        let end = start + arena.capacity as usize;
        let ptr_addr = ptr as usize;
        assert!(ptr_addr >= start);
        assert!(ptr_addr < end);
    }

    #[test]
    fn get_alloc_slice_matches_size() {
        let arena = Arena::new(4096);
        let data = [1u8, 2, 3, 4, 5];
        let alloc = arena.alloc_write(data).unwrap();
        let slice = arena.get_alloc_slice(&alloc);
        assert_eq!(slice.0.len(), 5);
        let read_back = unsafe { core::ptr::read(slice.0.as_ptr() as *const [u8; 5]) };
        assert_eq!(read_back, data);
    }

    #[test]
    fn dealloc_reclaims_top() {
        let arena = Arena::new(4096);
        let _alloc_a = arena.alloc_write(100u64).unwrap();
        let alloc_b = arena.alloc_write(200u64).unwrap();

        let offset_before = arena.offset.get();
        unsafe { arena.dealloc(alloc_b) };
        // after deallocating the top allocation, offset should shrink
        assert!(arena.offset.get() < offset_before);
    }

    #[test]
    fn dealloc_middle_does_not_reclaim() {
        let arena = Arena::new(4096);
        let _alloc_a = arena.alloc_write(10u64).unwrap();
        let alloc_b = arena.alloc_write(20u64).unwrap();
        let alloc_c = arena.alloc_write(30u64).unwrap();

        let offset_before = arena.offset.get();
        unsafe { arena.dealloc(alloc_b) };
        // B is not at the top, so offset stays unchanged
        assert_eq!(arena.offset.get(), offset_before);

        // Now deallocate C (top), B should still remain
        unsafe { arena.dealloc(alloc_c) };
        assert!(arena.offset.get() < offset_before);
    }

    #[test]
    fn dealloc_all_reclaims_everything() {
        let arena = Arena::new(4096);
        let alloc_a = arena.alloc_write(10u64).unwrap();
        let alloc_b = arena.alloc_write(20u64).unwrap();
        unsafe { arena.dealloc(alloc_b) };
        unsafe { arena.dealloc(alloc_a) };
        assert_eq!(arena.offset.get(), 0);
    }

    #[test]
    fn alloc_alignment() {
        let arena = Arena::new(4096);
        // alloc with 1-byte alignment
        let a1 = arena.alloc(Layout::from_size_align(1, 1).unwrap()).unwrap();
        assert_eq!(a1.offset % 1, 0);
        // alloc with 2-byte alignment
        let a2 = arena.alloc(Layout::from_size_align(1, 2).unwrap()).unwrap();
        assert_eq!(a2.offset % 2, 0);
        // alloc with 8-byte alignment
        let a3 = arena.alloc(Layout::from_size_align(1, 8).unwrap()).unwrap();
        assert_eq!(a3.offset % 8, 0);
        // alloc with 16-byte alignment
        let a4 = arena
            .alloc(Layout::from_size_align(1, 16).unwrap())
            .unwrap();
        assert_eq!(a4.offset % 16, 0);
    }

    #[test]
    fn alloc_alignment_advances_offset_correctly() {
        let arena = Arena::new(4096);
        // allocate 1 byte at offset 0
        let _a1 = arena.alloc(Layout::from_size_align(1, 1).unwrap()).unwrap();
        // allocate with 8-byte alignment – should skip to next 8-aligned offset
        let a2 = arena.alloc(Layout::from_size_align(4, 8).unwrap()).unwrap();
        assert_eq!(a2.offset % 8, 0);
    }
}
