use core::mem::MaybeUninit;

pub struct Slab<T> {
    slots: Vec<MaybeUninit<T>>,
    occupied: Box<[u64]>,
    free: Vec<u32>,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Slab<T> {
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            occupied: vec![].into_boxed_slice(),
            free: Vec::new(),
        }
    }

    pub fn insert(&mut self, value: T) -> Option<u32> {
        let index = if let Some(index) = self.free.pop() {
            index
        } else {
            self.find_free_slot()?
        };
        self.set_occupied(index, true);
        if index as usize >= self.slots.len() {
            self.grow(index as usize + 1);
        }
        self.slots[index as usize] = MaybeUninit::new(value);
        Some(index)
    }

    pub fn insert_at(&mut self, index: u32, value: T) {
        if index as usize >= self.slots.len() {
            self.grow(index as usize + 1);
        }
        self.set_occupied(index, true);
        self.slots[index as usize] = MaybeUninit::new(value);
    }

    pub fn remove(&mut self, index: u32) -> T {
        self.set_occupied(index, false);
        self.free.push(index);
        unsafe { self.slots[index as usize].assume_init_read() }
    }

    pub fn get(&self, index: u32) -> Option<&T> {
        if self.is_occupied(index) {
            Some(unsafe { self.slots[index as usize].assume_init_ref() })
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, index: u32) -> Option<&mut T> {
        if self.is_occupied(index) {
            Some(unsafe { self.slots[index as usize].assume_init_mut() })
        } else {
            None
        }
    }

    pub fn contains(&self, index: u32) -> bool {
        self.is_occupied(index)
    }

    fn find_free_slot(&self) -> Option<u32> {
        for (word_idx, &word) in self.occupied.iter().enumerate() {
            if word != u64::MAX {
                let bit = (!word).trailing_zeros();
                let index = word_idx as u32 * 64 + bit;
                return Some(index);
            }
        }
        // All existing words full — next slot is at the end
        let index = self.occupied.len() as u32 * 64;
        Some(index)
    }

    fn grow(&mut self, min_capacity: usize) {
        let new_len = min_capacity.max(self.slots.len().max(8) * 2);
        self.slots.resize_with(new_len, MaybeUninit::uninit);
        let words_needed = new_len.div_ceil(64);
        if words_needed > self.occupied.len() {
            let mut new_occupied = vec![0u64; words_needed].into_boxed_slice();
            new_occupied[..self.occupied.len()].copy_from_slice(&self.occupied);
            self.occupied = new_occupied;
        }
    }

    fn is_occupied(&self, index: u32) -> bool {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        word < self.occupied.len() && self.occupied[word] & (1 << bit) != 0
    }

    fn set_occupied(&mut self, index: u32, occupied: bool) {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        if word >= self.occupied.len() {
            let new_len = word + 1;
            let mut new_occupied = vec![0u64; new_len].into_boxed_slice();
            new_occupied[..self.occupied.len()].copy_from_slice(&self.occupied);
            self.occupied = new_occupied;
        }
        if occupied {
            self.occupied[word] |= 1 << bit;
        } else {
            self.occupied[word] &= !(1 << bit);
        }
    }
}
