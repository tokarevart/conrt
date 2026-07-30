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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_sequential() {
        let mut slab = Slab::new();
        assert_eq!(slab.insert(10), Some(0));
        assert_eq!(slab.insert(20), Some(1));
        assert_eq!(slab.insert(30), Some(2));
    }

    #[test]
    fn remove_reuses_slot() {
        let mut slab = Slab::new();
        slab.insert(100);
        slab.insert(200);
        assert_eq!(slab.remove(0), 100);
        assert!(!slab.contains(0));
        assert_eq!(slab.insert(300), Some(0));
        assert!(slab.contains(0));
    }

    #[test]
    fn insert_at_and_contains() {
        let mut slab = Slab::new();
        slab.insert_at(42, 99);
        assert!(slab.contains(42));
        assert_eq!(slab.slots.len(), 43);
    }

    #[test]
    fn insert_at_preserves_capacity() {
        let mut slab = Slab::new();
        slab.insert(10);
        slab.insert(20);
        slab.insert(30);
        slab.insert_at(99, 999);
        // remove should still find free slot <64 if one exists
        slab.remove(0);
        assert_eq!(slab.insert(555), Some(0));
    }

    #[test]
    fn inline_free_overflow() {
        let mut slab = Slab::new();
        // fill all 64 inline slots
        for i in 0..64 {
            assert_eq!(slab.insert(i as u64), Some(i));
        }
        // inline_free should be 0 now
        assert_eq!(slab.inline_free, 0);
        // next insertion extends the slab
        assert_eq!(slab.insert(64), Some(64));
        // removing index >= 64 pushes to free vec
        slab.remove(64);
        assert_eq!(slab.free.len(), 1);
        // and a new insert reuses that heap slot
        assert_eq!(slab.insert(65), Some(64));
    }

    #[test]
    fn remove_updates_free_list_inline() {
        let mut slab = Slab::new();
        for i in 0..10 {
            slab.insert(i);
        }
        slab.remove(0);
        slab.remove(5);
        assert!(!slab.contains(0));
        assert!(!slab.contains(5));
        // Re-insert reuses smallest free inline slot
        assert_eq!(slab.insert(100), Some(0));
        assert_eq!(slab.insert(200), Some(5));
    }

    #[test]
    fn remove_frees_slot_and_allows_reuse() {
        let mut slab = Slab::<u64>::new();
        slab.insert(10);
        slab.insert(20);
        slab.remove(1);
        assert!(!slab.contains(1));
        assert_eq!(slab.insert(30), Some(1));
    }
}
