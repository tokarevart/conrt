use core::mem::MaybeUninit;

pub struct Slab<T> {
    slots: Vec<MaybeUninit<T>>,
    inline_free: u64,
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
            inline_free: u64::MAX,
            free: Vec::new(),
        }
    }

    pub fn insert(&mut self, value: T) -> Option<u32> {
        let index = self.find_free_slot()?;
        if index as usize >= self.slots.len() {
            self.slots
                .resize_with(index as usize + 1, MaybeUninit::uninit);
        }
        self.slots[index as usize] = MaybeUninit::new(value);
        Some(index)
    }

    pub fn insert_at(&mut self, index: u32, value: T) {
        let idx = index as usize;
        if idx >= self.slots.len() {
            let old_len = self.slots.len();
            self.slots.resize_with(idx + 1, MaybeUninit::uninit);
            for i in old_len..(idx + 1).min(64) {
                self.inline_free |= 1 << i;
            }
        }
        self.mark_occupied(index);
        self.slots[idx] = MaybeUninit::new(value);
    }

    pub fn remove(&mut self, index: u32) -> T {
        self.mark_free(index);
        unsafe { self.slots[index as usize].assume_init_read() }
    }

    pub fn contains(&self, index: u32) -> bool {
        !self.is_free(index)
    }

    fn find_free_slot(&mut self) -> Option<u32> {
        if self.inline_free != 0 {
            let i = self.inline_free.trailing_zeros();
            self.inline_free &= !(1 << i);
            return Some(i);
        }
        if let Some(i) = self.free.pop() {
            return Some(i);
        }
        let i = self.slots.len() as u32;
        self.slots.push(MaybeUninit::uninit());
        if i < 64 {
            self.inline_free &= !(1 << i);
        }
        Some(i)
    }

    fn mark_free(&mut self, index: u32) {
        if index < 64 {
            self.inline_free |= 1 << index;
        } else {
            self.free.push(index);
        }
    }

    fn mark_occupied(&mut self, index: u32) {
        if index < 64 {
            self.inline_free &= !(1 << index);
        } else {
            self.free.retain(|&i| i != index);
        }
    }

    fn is_free(&self, index: u32) -> bool {
        if index < 64 {
            self.inline_free & (1 << index) != 0
        } else {
            self.free.contains(&index)
        }
    }
}
