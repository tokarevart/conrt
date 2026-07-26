extern crate alloc;

use core::alloc::Layout;
use core::ptr::NonNull;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::cell::Cell;
use std::num::NonZeroU32;

pub const ARENA_MAX: u32 = u32::MAX;
pub const FOOTER_SIZE: u32 = size_of::<AllocFooter>() as u32;

#[repr(C, packed)]
struct AllocFooter {
    pub size: u32,
    pub deallocated: u8,
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
    base_ptr: *mut u8,
    offset: Cell<u32>,
    max_capacity: u32,
}

impl Arena {
    pub fn new(capacity: u32) -> Self {
        let layout = Layout::from_size_align(capacity as usize, 16).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Self {
            base_ptr: ptr,
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
        let footer = unsafe { &mut *(self.base_ptr.add(padded as usize) as *mut AllocFooter) };
        footer.size = padded - self.offset.get();
        footer.deallocated = 0;
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
        unsafe { self.base_ptr.add(alloc.offset as usize) }
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
        let ptr = self.ptr_for(&alloc);
        let footer = unsafe { &mut *(ptr.add(alloc.size.get() as usize) as *mut AllocFooter) };
        footer.deallocated = 1;

        while self.offset.get() > FOOTER_SIZE {
            let top = unsafe {
                &mut *(self
                    .base_ptr
                    .add((self.offset.get() - FOOTER_SIZE) as usize)
                    as *mut AllocFooter)
            };
            if top.deallocated == 0 {
                break;
            }
            let size = top.size;
            self.offset.set(self.offset.get() - FOOTER_SIZE - size);
            let new_top = unsafe {
                &mut *(self
                    .base_ptr
                    .add((self.offset.get() - FOOTER_SIZE) as usize)
                    as *mut AllocFooter)
            };
            new_top.deallocated = 0;
        }
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.max_capacity as usize, 16).unwrap();
        unsafe { dealloc(self.base_ptr, layout) };
    }
}
