//! The shared buffer slab: its pools, the thread-local that publishes them to
//! buffer views, and the `bid` routing every borrow operation goes through.
//!
//! This is the io_uring backend's private half — the whole of it is what a
//! provided read fills and what a fixed write addresses, which is why it has no
//! presence in `coio-core`.

use core::cell::Cell;
use core::num::NonZeroU32;
use std::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::ptr::NonNull;

use coio_core::active_gen_matches;
use coio_core::is_running;

use crate::buf::FixedPool;
use crate::buf::FixedSlab;
use crate::buf::ProvidedPool;
use crate::buf::ProvidedSlab;
use crate::buf::pool::Slab;
use crate::classes::BUFFER_MAX_ALIGN;
use crate::classes::SizeClass;
use crate::classes::bid_class;
use crate::classes::bid_local;
use crate::classes::bid_provided;
use crate::classes::layout_classes;

thread_local! {
    /// Published by [`crate::uring::block_on`] for as long as a runtime is
    /// running, so a buffer view can resolve its `bid` without naming the
    /// runtime's `G`/`L`.
    static CURRENT_POOLS: Cell<*mut Pools> = const { Cell::new(core::ptr::null_mut()) };
}

pub(crate) fn set_current_pools(pools: *mut Pools) {
    CURRENT_POOLS.with(|c| c.set(pools));
}

pub(crate) fn clear_current_pools() {
    CURRENT_POOLS.with(|c| c.set(core::ptr::null_mut()));
}

/// Gives `f` mutable access to the active runtime's buffer pools. `Pools` is
/// the non-generic half of a runtime, so buffer views can reach their slabs
/// without naming the runtime's `G`/`L`.
pub(crate) fn with_pools<R>(f: impl FnOnce(&mut Pools) -> R) -> R {
    assert!(is_running(), "with_pools called outside an active runtime");
    let ptr = CURRENT_POOLS.with(|c| c.get());
    assert!(!ptr.is_null(), "no active runtime");
    unsafe { f(&mut *ptr) }
}
// ── View routing ─────────────────────────────────────────────────────
//
// The [`crate::buf`] view types never reach into the pools directly: every
// borrow operation is dispatched here on the packed `bid` (provided vs. fixed
// pool). A view whose generation no longer matches the active runtime is
// stale — its runtime has shut down and its slot may have been freed — so
// drop and clone are no-ops (the slot is leaked rather than touched), while
// resolve and the exclusive transitions panic.

/// Resolves a view's guarded slot to the start of its memory plus the view's
/// sub-slot byte `offset`. Panics when called outside the runtime that owns
/// the slot.
pub(crate) fn resolve_ptr(bid: u32, generation: NonZeroU32, offset: u32) -> NonNull<u8> {
    assert!(
        active_gen_matches(generation),
        "resolve_ptr called outside the runtime that owns this buffer"
    );
    with_pools(|p| {
        let base = p.with_slab(bid, |slab, local| slab.slot_ptr(local));
        unsafe { base.add(offset as usize) }
    })
}

/// Registers one more shared borrower: a cloned [`crate::buf::Ref`]. A stale
/// view is dropped after its runtime shut down and leaks its slot.
pub(crate) fn clone_view(bid: u32, generation: NonZeroU32) {
    if !active_gen_matches(generation) {
        return;
    }
    with_pools(|p| p.with_slab(bid, |slab, local| slab.tracker_mut().clone_shared(local)))
}

/// Releases one borrower of a view's slot, recycling the slot when the last
/// view drops. `exclusive` selects the exclusive (`RefMut`/`SliceMut`) vs.
/// shared (`Ref`/`Slice`) release. A stale view leaks its slot instead.
pub(crate) fn drop_view(bid: u32, generation: NonZeroU32, exclusive: bool) {
    if !active_gen_matches(generation) {
        return;
    }
    with_pools(|p| p.with_slab(bid, |slab, local| slab.drop_view(exclusive, local)))
}

/// Flips a sole shared `Ref` to an exclusive `RefMut`. Panics unless the slot
/// has exactly one shared holder.
pub(crate) fn upgrade_view(bid: u32, generation: NonZeroU32) {
    assert!(
        active_gen_matches(generation),
        "upgrade_view called outside the runtime that owns this buffer"
    );
    with_pools(|p| p.with_slab(bid, |slab, local| slab.tracker_mut().upgrade(local)))
}

/// Flips a sole exclusive `RefMut` to a shared `Ref`. Panics unless the slot
/// has exactly one exclusive holder.
pub(crate) fn downgrade_view(bid: u32, generation: NonZeroU32) {
    assert!(
        active_gen_matches(generation),
        "downgrade_view called outside the runtime that owns this buffer"
    );
    with_pools(|p| p.with_slab(bid, |slab, local| slab.tracker_mut().downgrade(local)))
}

/// Runs a closure exactly once when dropped, even on panic unwind.
pub(crate) struct DropGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> DropGuard<F> {
    pub(crate) fn new(f: F) -> Self {
        Self(Some(f))
    }
}

impl<F: FnOnce()> Drop for DropGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f()
        }
    }
}

/// The buffer-pool half of a runtime: the shared buffer slab plus the provided
/// and fixed pools built over it. Deliberately non-generic — buffer views
/// reach it through `with_pools` without naming the runtime's `G`/`L`.
pub struct Pools {
    /// Start of the shared buffer slab, allocated aligned to at most one page.
    pub slab: NonNull<u8>,
    /// The layout of the slab allocation, used to free it on drop.
    slab_layout: Layout,
    pub(crate) provided_pool: ProvidedPool,
    pub(crate) fixed_pool: FixedPool,
}

impl Drop for Pools {
    fn drop(&mut self) {
        unsafe { dealloc(self.slab.as_ptr(), self.slab_layout) };
    }
}

impl Pools {
    /// Hands the closure the slab of the `bid`'s direction and class, plus its
    /// slot local id, decoding `bid` once for all borrow operations.
    pub(crate) fn with_slab<R>(&mut self, bid: u32, f: impl FnOnce(&mut dyn Slab, u32) -> R) -> R {
        let class = bid_class(bid);
        let local = bid_local(bid);
        if bid_provided(bid) {
            f(self.provided_pool.slab_mut(class), local)
        } else {
            f(self.fixed_pool.slab_mut(class), local)
        }
    }
}

/// Builds both directions' pools over one shared slab: the provided classes
/// occupy the start of the slab, the fixed classes follow them. Each provided
/// class gets a provided-buffer ring registered under `bgid = class index`;
/// the fixed classes get free-stack slabs, and the fixed region alone is
/// registered as fixed buffer index 0 so writes can address any slot by its
/// absolute offset. Returns the slab and both pools.
///
/// The slab is allocated aligned to at most one page (4 KiB) and the classes
/// are laid out on offsets aligned to their own sizes capped at that page
/// alignment, so every buffer is aligned to `min(size, 4096)` and no buffer
/// claims an alignment beyond a page. Class sizes must be powers of two (the
/// allocator only guarantees power-of-two alignment).
pub(crate) fn build_pools(
    ring: &io_uring::IoUring,
    provided_spec: &[SizeClass],
    fixed_spec: &[SizeClass],
) -> Pools {
    let provided_layout = layout_classes(provided_spec, true);
    let fixed_layout = layout_classes(fixed_spec, false);
    let provided_total = provided_layout.last().map(|l| l.total).unwrap();
    let align = provided_spec
        .iter()
        .chain(fixed_spec)
        .map(|class| class.size.min(BUFFER_MAX_ALIGN) as usize)
        .max()
        .expect("at least one size class is required");
    assert!(
        align.is_power_of_two(),
        "buffer size class sizes must be powers of two so buffers can be aligned to their size"
    );
    let fixed_start = provided_total.div_ceil(u64::from(align as u32)) * u64::from(align as u32);
    let total = fixed_start + fixed_layout.last().map(|l| l.total).unwrap();
    assert!(
        total <= u64::from(u32::MAX),
        "the combined provided+fixed slab exceeds the 4 GiB offset range"
    );

    let layout = Layout::from_size_align(total as usize, align).unwrap();
    let slab = unsafe { alloc(layout) };
    if slab.is_null() {
        handle_alloc_error(layout);
    }
    let base = unsafe { NonNull::new_unchecked(slab) };

    let provided_classes: Vec<SizeClass> = provided_layout
        .iter()
        .map(|l| SizeClass {
            size: l.size,
            count: l.count,
        })
        .collect();
    let mut provided_slabs = Vec::with_capacity(provided_layout.len());
    for (i, l) in provided_layout.iter().enumerate() {
        let class_base = unsafe { base.as_ptr().add(l.base_offset as usize) };
        let slab = ProvidedSlab::new(
            unsafe { NonNull::new_unchecked(class_base) },
            l.count as u16,
            l.size,
            i as _,
        );
        provided_slabs.push(slab);
    }
    let provided_pool = ProvidedPool::new(provided_classes, provided_slabs);
    provided_pool
        .register_all(ring)
        .expect("failed to register the provided buffer rings");

    let fixed_classes: Vec<SizeClass> = fixed_layout
        .iter()
        .map(|l| SizeClass {
            size: l.size,
            count: l.count,
        })
        .collect();
    let fixed_slabs: Vec<FixedSlab> = fixed_layout
        .iter()
        .enumerate()
        .map(|(i, l)| {
            let class_base = unsafe {
                base.as_ptr()
                    .add((fixed_start + u64::from(l.base_offset)) as usize)
            };
            FixedSlab::new(
                unsafe { NonNull::new_unchecked(class_base) },
                l.size,
                l.count,
                i as u8,
            )
        })
        .collect();
    let fixed_pool = FixedPool::new(fixed_classes, fixed_slabs);

    // The whole slab — provided region and fixed region alike — is registered
    // as fixed buffer index 0, so `write_fixed` can address a slot from either
    // region by absolute address. That is what lets data read into a provided
    // buffer be written straight back out to another fd without first copying
    // it into the fixed region.
    //
    // The cost is locked memory. `IORING_REGISTER_BUFFERS` locks the pages it
    // covers 1:1 against the *user's* `locked_vm` for `RLIMIT_MEMLOCK` — a
    // per-UID budget shared with every other process of that user, and not
    // observable as `VmLck` in /proc/self/status because io_uring charges the
    // user counter, not the mm's. A provided buffer is only *described* to the
    // kernel by its registered pbuf ring, so its pages are not otherwise
    // locked, and this registration is what locks them.
    //
    // Registering only the fixed region halves the charge but makes provided
    // buffers unaddressable by `write_fixed`. That is why the default size
    // classes are halved (see `DEFAULT_SIZE_CLASSES`): a runtime then charges
    // about as much as it used to, and the whole slab fits where the fixed
    // region used to. `default_params_runtime_fits_in_memlock` in the test
    // suite fails with the kernel's `ENOMEM` if a host cannot afford it.
    let iovec = libc::iovec {
        iov_base: base.as_ptr().cast(),
        iov_len: total as usize,
    };
    unsafe { ring.submitter().register_buffers(&[iovec]) }
        .expect("failed to register the buffer slab as fixed buffer index 0");

    Pools {
        slab: base,
        slab_layout: layout,
        provided_pool,
        fixed_pool,
    }
}
