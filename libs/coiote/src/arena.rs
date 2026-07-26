extern crate alloc;

use core::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;

pub const ARENA_MAX: u32 = u32::MAX;
pub const FOOTER_SIZE: u32 = core::mem::size_of::<AllocFooter>() as u32;

#[repr(C, packed)]
pub struct AllocFooter {
    pub size: u32,
    pub deallocated: u8,
}

pub struct Arena {
    base_ptr: *mut u8,
    offset: u32,
    max_capacity: u32,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub fn new(capacity: u32) -> Self {
        let layout = Layout::from_size_align(capacity as usize, 16).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Self {
            base_ptr: ptr,
            offset: 0,
            max_capacity: capacity,
        }
    }

    pub fn base_ptr(&self) -> *mut u8 {
        self.base_ptr
    }

    pub fn alloc(&mut self, layout: Layout) -> Option<ArenaAlloc<'_>> {
        let align = layout.align() as u32;
        let size = layout.size() as u32;
        let aligned = (self.offset + align - 1) & !(align - 1);
        let padded = aligned + size;
        let total = padded.checked_add(FOOTER_SIZE)?;
        if total > self.max_capacity {
            return None;
        }
        let footer = unsafe { &mut *(self.base_ptr.add(padded as usize) as *mut AllocFooter) };
        footer.size = padded - self.offset;
        footer.deallocated = 0;
        self.offset = padded + FOOTER_SIZE;
        Some(ArenaAlloc {
            arena: self,
            alloc_offset: aligned,
            alloc_size: size,
        })
    }

    pub fn alloc_bytes(&mut self, len: u32) -> Option<ArenaAlloc<'_>> {
        self.alloc(Layout::from_size_align(len as usize, align_of::<u8>()).unwrap())
    }

    pub fn alloc_type<T>(&mut self) -> Option<ArenaAlloc<'_>> {
        self.alloc(Layout::new::<T>())
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.max_capacity as usize, 16).unwrap();
        unsafe { dealloc(self.base_ptr, layout) };
    }
}

pub struct ArenaAlloc<'a> {
    arena: &'a mut Arena,
    pub alloc_offset: u32,
    pub alloc_size: u32,
}

impl<'a> Drop for ArenaAlloc<'a> {
    #[inline(always)]
    fn drop(&mut self) {
        let footer = unsafe {
            &mut *(self
                .arena
                .base_ptr
                .add(self.alloc_offset as usize)
                .add(self.alloc_size as usize) as *mut AllocFooter)
        };
        footer.deallocated = 1;

        while self.arena.offset > FOOTER_SIZE {
            let top = unsafe {
                &mut *(self
                    .arena
                    .base_ptr
                    .add((self.arena.offset - FOOTER_SIZE) as usize)
                    as *mut AllocFooter)
            };
            if top.deallocated == 0 {
                break;
            }
            let size = top.size;
            self.arena.offset -= FOOTER_SIZE + size;
            let new_top = unsafe {
                &mut *(self
                    .arena
                    .base_ptr
                    .add((self.arena.offset - FOOTER_SIZE) as usize)
                    as *mut AllocFooter)
            };
            new_top.deallocated = 0;
        }
    }
}
