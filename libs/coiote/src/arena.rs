extern crate alloc;

use core::alloc::Layout;
use core::ptr::NonNull;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::cell::Cell;
use std::num::NonZeroU32;

pub const ARENA_MAX: u32 = u32::MAX;
pub const FOOTER_SIZE: u32 = size_of::<AllocFooter>() as u32;
pub const PAGE_SIZE: usize = 4096;

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

const _: () = assert!(size_of::<Option<ArenaAlloc>>() == size_of::<ArenaAlloc>());

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct ArenaAllocSlice(pub NonNull<[u8]>);

pub struct Arena {
    base_ptr: NonNull<u8>, // page aligned
    offset: Cell<u32>,
    max_capacity: u32,
}

impl Arena {
    pub fn new(capacity: u32) -> Self {
        let layout = Layout::from_size_align(capacity as usize, PAGE_SIZE).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self {
            base_ptr: unsafe { NonNull::new_unchecked(ptr) },
            offset: Cell::new(0),
            max_capacity: capacity,
        }
    }

    pub fn alloc(&self, layout: Layout) -> Option<ArenaAlloc> {
        let size = NonZeroU32::new(layout.size() as u32)?;

        let align = layout.align() as u32;
        let aligned = (self.offset.get() + align - 1) & !(align - 1);
        let padded = aligned + size.get();
        let total = padded.checked_add(FOOTER_SIZE)?;
        if total > self.max_capacity {
            return None;
        }
        let footer_addr = unsafe {
            self.base_ptr
                .as_ptr()
                .add(padded as usize)
                .cast::<AllocFooter>()
        };
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
        unsafe { self.base_ptr.as_ptr().add(alloc.offset as usize) }
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
                    .base_ptr
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
        let layout = Layout::from_size_align(self.max_capacity as usize, 16).unwrap();
        unsafe { dealloc(self.base_ptr.as_ptr(), layout) };
    }
}
