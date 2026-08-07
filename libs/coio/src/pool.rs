//! Per-slot borrow accounting shared by the fixed and provided buffer pools.
//!
//! Both [`crate::buf::BufferPool`] and [`crate::pbuf::ProvidedBufferPool`]
//! hand out views over slots of a shared slab and must track how many views
//! (shared or exclusive) currently hold each slot, recycling it only when the
//! last view drops. The transition logic is identical in both pools, so it
//! lives here once: a `BorrowTracker` owns the per-slot count and every state
//! change, and the pools decide what "recycle" means (`None` here — that is
//! the pool's `drop_view` wrapper, which pushes the slot back to its free
//! stack or kernel ring when `drop_view` reports the last release).
//!
//! Slots are addressed by `u32` ids — the same width the packed `bid` carries
//! (only the low 24 bits are used) — so the tracker needs no per-pool
//! knowledge of their narrower kernel-facing ids.

pub(crate) struct BorrowTracker {
    /// Per-slot borrow count: `0` means free, positive values count shared
    /// views, negative values count exclusive views.
    borrows: Vec<i32>,
}

impl BorrowTracker {
    /// One zeroed count per slot.
    pub(crate) fn new(count: usize) -> Self {
        Self {
            borrows: vec![0; count],
        }
    }

    /// Marks a free slot (`0`) shared (`+1`). Panics if it is already
    /// borrowed. This is the root of a shared view, e.g. a cloned [`Ref`]
    /// or the `Bytes` handed to a `BUFFER_SELECT` read result.
    pub(crate) fn take_shared(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert!(
            *borrow == 0,
            "take_shared: slot {local} is already borrowed"
        );
        *borrow = 1;
    }

    /// Marks a free slot (`0`) exclusively borrowed (`−1`). Panics if it is
    /// already borrowed. This is the root of an exclusive view, e.g. a
    /// `BytesMut` handed out for a write.
    pub(crate) fn take_exclusive(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert!(
            *borrow == 0,
            "take_exclusive: slot {local} is already borrowed"
        );
        *borrow = -1;
    }

    /// Registers one more shared borrower (a cloned view). Panics if the slot
    /// is not currently shared.
    pub(crate) fn clone_shared(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert!(*borrow > 0, "clone_shared: slot {local} is not shared");
        *borrow += 1;
    }

    /// Registers one more exclusive borrower (a split of an exclusive view).
    /// Panics if the slot is not currently exclusive.
    pub(crate) fn split_exclusive(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert!(
            *borrow < 0,
            "split_exclusive: slot {local} is not exclusively borrowed"
        );
        *borrow -= 1;
    }

    /// Shared `+1` → exclusive `−1`. Panics unless this slot has exactly one
    /// shared holder.
    pub(crate) fn upgrade(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert_eq!(
            *borrow, 1,
            "upgrade: slot {local} must have exactly one shared holder"
        );
        *borrow = -1;
    }

    /// Exclusive `−1` → shared `+1`. Panics unless this slot has exactly one
    /// exclusive holder.
    pub(crate) fn downgrade(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert_eq!(
            *borrow, -1,
            "downgrade: slot {local} must have exactly one exclusive holder"
        );
        *borrow = 1;
    }

    /// Releases one borrower of `local`, returning `true` when the slot is
    /// fully free again so the owning pool can recycle it. Panics on a count
    /// mismatch (an over-release, a double-drop, or releasing the wrong kind
    /// of borrow).
    pub(crate) fn drop_view(&mut self, exclusive: bool, local: u32) -> bool {
        let borrow = &mut self.borrows[local as usize];
        assert!(*borrow != 0, "drop_view: slot {local} is not borrowed");
        if exclusive {
            assert!(
                *borrow < 0,
                "drop_view: exclusive release of a shared borrow on slot {local}"
            );
            *borrow += 1;
        } else {
            assert!(
                *borrow > 0,
                "drop_view: shared release of an exclusive borrow on slot {local}"
            );
            *borrow -= 1;
        }
        *borrow == 0
    }

    #[cfg(test)]
    pub(crate) fn borrows(&self, local: u32) -> i32 {
        self.borrows[local as usize]
    }
}
