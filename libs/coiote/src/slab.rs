use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use std::marker::PhantomData;

#[derive(Default)]
pub struct Slab<T> {
    slots: UnsafeCell<Vec<MaybeUninit<T>>>,
    occupied: UnsafeCell<Vec<u64>>,
    free: UnsafeCell<Vec<u32>>,
    _not_send_sync: PhantomData<*mut T>,
}

impl<T> Slab<T> {
    pub fn new() -> Self {
        Self {
            slots: UnsafeCell::new(Vec::new()),
            occupied: UnsafeCell::new(Vec::new()),
            free: UnsafeCell::new(Vec::new()),
            _not_send_sync: PhantomData,
        }
    }

    pub fn insert(&self, value: T) -> Option<u32> {
        let free = unsafe { &mut *self.free.get() };
        let index = if let Some(index) = free.pop() {
            index
        } else {
            self.find_free_slot()?
        };

        self.set_occupied(index, true);

        let slots = unsafe { &mut *self.slots.get() };
        if index as usize >= slots.len() {
            self.grow(index as usize + 1);
        }

        // Re-borrow slots in case `grow` reallocated the vector buffer
        let slots = unsafe { &mut *self.slots.get() };
        slots[index as usize] = MaybeUninit::new(value);
        Some(index)
    }

    pub fn insert_at(&self, index: u32, value: T) {
        let slots = unsafe { &mut *self.slots.get() };
        if index as usize >= slots.len() {
            self.grow(index as usize + 1);
        }

        self.set_occupied(index, true);

        let slots = unsafe { &mut *self.slots.get() };
        slots[index as usize] = MaybeUninit::new(value);
    }

    pub fn remove(&self, index: u32) -> T {
        self.set_occupied(index, false);

        let free = unsafe { &mut *self.free.get() };
        free.push(index);

        let slots = unsafe { &mut *self.slots.get() };
        unsafe { slots[index as usize].assume_init_read() }
    }

    pub fn contains(&self, index: u32) -> bool {
        self.is_occupied(index)
    }

    fn find_free_slot(&self) -> Option<u32> {
        let occupied = unsafe { &*self.occupied.get() };
        for (word_idx, &word) in occupied.iter().enumerate() {
            if word != u64::MAX {
                let bit = (!word).trailing_zeros();
                let index = word_idx as u32 * 64 + bit;
                return Some(index);
            }
        }
        // All existing words full — next slot is at the end
        let index = occupied.len() as u32 * 64;
        Some(index)
    }

    fn grow(&self, min_capacity: usize) {
        let slots = unsafe { &mut *self.slots.get() };
        let occupied = unsafe { &mut *self.occupied.get() };

        let new_len = min_capacity.max(slots.len().max(8) * 2);
        slots.resize_with(new_len, MaybeUninit::uninit);

        let words_needed = new_len.div_ceil(64);
        if words_needed > occupied.len() {
            occupied.resize(words_needed, 0);
        }
    }

    fn is_occupied(&self, index: u32) -> bool {
        let occupied = unsafe { &*self.occupied.get() };
        let word = index as usize / 64;
        let bit = index as usize % 64;
        word < occupied.len() && occupied[word] & (1 << bit) != 0
    }

    fn set_occupied(&self, index: u32, is_occ: bool) {
        let occupied = unsafe { &mut *self.occupied.get() };
        let word = index as usize / 64;
        let bit = index as usize % 64;

        if word >= occupied.len() {
            let new_len = word + 1;
            occupied.resize(new_len, 0);
        }

        if is_occ {
            occupied[word] |= 1 << bit;
        } else {
            occupied[word] &= !(1 << bit);
        }
    }
}

impl<T> Drop for Slab<T> {
    fn drop(&mut self) {
        // UnsafeCell::get_mut gives us safe &mut access during Drop
        let slots = self.slots.get_mut();
        let occupied = self.occupied.get_mut();

        for (word_idx, &word) in occupied.iter().enumerate() {
            if word == 0 {
                continue;
            }

            let mut bits = word;
            while bits != 0 {
                // Find index of the lowest set bit
                let bit = bits.trailing_zeros();
                let index = word_idx * 64 + bit as usize;

                if index < slots.len() {
                    unsafe {
                        slots[index].assume_init_drop();
                    }
                }

                // Clear the bit we just processed
                bits &= bits - 1;
            }
        }
    }
}
