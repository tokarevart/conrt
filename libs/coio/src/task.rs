use core::any::TypeId;
use core::cell::Cell;
use core::fmt;
use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::Context;
use core::task::Poll;
use std::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::io;
use std::num::NonZeroU64;

use io_uring::squeue;

use crate::runtime;
use crate::runtime::Runtime;

thread_local! {
    static NEXT_TASK_ID: Cell<NonZeroU64> = const { Cell::new(NonZeroU64::new(1).unwrap()) };
}

/// One in-flight IO op's state, stored in the runtime's global io slab. The
/// slot is only ever touched through Rust code, so no stable layout is
/// required; the fields are ordered to land on exactly 8 bytes.
#[derive(Debug, Clone, Copy, Default)]
pub struct IoSlot {
    pub result: i32,
    pub submitted: u8,
    pub ready: u8,
    pub bid: u16,
}

const IO_HEAP_FLAG: u32 = 1 << 31;
const IO_INLINE_CAP: usize = 3;

/// Inline arm of `IoVecRepr`. `len` is the first field so it aliases the first
/// field of the heap arm: the switch bit (`IO_HEAP_FLAG`) and the low 31-bit
/// logical length can always be read from offset zero, whichever arm is live.
#[repr(C)]
#[derive(Clone, Copy)]
struct IoVecInline {
    len: u32,
    inline: [u32; IO_INLINE_CAP],
}

/// Heap arm of `IoVecRepr`. `len` sits at the same offset as in
/// `IoVecInline`; `cap` fills what would otherwise be padding before the
/// 8-byte-aligned pointer. The union is exactly 16 bytes.
#[repr(C)]
#[derive(Clone, Copy)]
struct IoVecHeap {
    len: u32,
    cap: u32,
    ptr: NonNull<u32>,
}

/// A compact set of the io slab slots a task currently holds (its in-flight
/// ops). Up to three slot indices are stored inline; beyond that the indices
/// move to a heap allocation. The switch bit is the highest bit of `len`: when
/// set, the heap arm of the union is live.
#[repr(C)]
pub union IoVec {
    inline: IoVecInline,
    heap: IoVecHeap,
}

impl IoVec {
    pub fn new() -> Self {
        Self {
            inline: IoVecInline {
                len: 0,
                inline: [0; IO_INLINE_CAP],
            },
        }
    }

    fn len_field(&self) -> u32 {
        // `len` is the first field of both arms, so reading via the inline arm
        // is well-defined (u32 has no invalid bit patterns) regardless of the
        // live variant.
        unsafe { self.inline.len }
    }

    fn set_len_field(&mut self, len: u32) {
        self.inline.len = len;
    }

    pub fn len(&self) -> usize {
        (self.len_field() & !IO_HEAP_FLAG) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_heap(&self) -> bool {
        self.len_field() & IO_HEAP_FLAG != 0
    }

    fn capacity(&self) -> usize {
        if self.is_heap() {
            unsafe { self.heap.cap as usize }
        } else {
            IO_INLINE_CAP
        }
    }

    fn slot_at(&self, i: usize) -> u32 {
        unsafe {
            if self.is_heap() {
                *self.heap.ptr.as_ptr().add(i)
            } else {
                self.inline.inline[i]
            }
        }
    }

    fn slot_at_mut(&mut self, i: usize) -> &mut u32 {
        unsafe {
            if self.is_heap() {
                &mut *self.heap.ptr.as_ptr().add(i)
            } else {
                &mut self.inline.inline[i]
            }
        }
    }

    /// Appends `index`, growing to a heap allocation past the inline capacity.
    pub fn push(&mut self, index: u32) {
        let len = self.len();
        if !self.is_heap() {
            if len < IO_INLINE_CAP {
                unsafe {
                    self.inline.inline[len] = index;
                }
                self.set_len_field(self.len_field() + 1);
                return;
            }
            self.grow_to_heap();
        }
        if len == self.capacity() {
            self.grow_heap();
        }
        *self.slot_at_mut(len) = index;
        self.set_len_field(self.len_field() + 1);
    }

    /// Removes the first occurrence of `index`, swapping in the last element.
    /// Returns whether the index was present. Order is irrelevant: the set of
    /// in-flight slot indices is unordered.
    pub fn remove_value(&mut self, index: u32) -> bool {
        let len = self.len();
        for i in 0..len {
            if self.slot_at(i) == index {
                let last = len - 1;
                if i != last {
                    let last_value = self.slot_at(last);
                    *self.slot_at_mut(i) = last_value;
                }
                self.set_len_field(self.len_field() - 1);
                return true;
            }
        }
        false
    }

    pub fn iter(&self) -> IoVecIter<'_> {
        IoVecIter {
            vec: self,
            i: 0,
            len: self.len(),
        }
    }

    fn grow_to_heap(&mut self) {
        let new_cap = (IO_INLINE_CAP as u32).max(4);
        let layout = Layout::array::<u32>(new_cap as usize).unwrap();
        let ptr = unsafe { alloc(layout) } as *mut u32;
        assert!(!ptr.is_null(), "io vec allocation failed");
        let len = self.len_field() | IO_HEAP_FLAG;
        unsafe {
            core::ptr::copy_nonoverlapping(self.inline.inline.as_ptr(), ptr, IO_INLINE_CAP);
            self.heap = IoVecHeap {
                len,
                cap: new_cap,
                ptr: NonNull::new_unchecked(ptr),
            };
        }
    }

    fn grow_heap(&mut self) {
        let (old_ptr, old_cap) = unsafe { (self.heap.ptr, self.heap.cap) };
        let old_cap = old_cap as usize;
        let new_cap = old_cap * 2;
        let layout = Layout::array::<u32>(old_cap).unwrap();
        let new_layout = Layout::array::<u32>(new_cap).unwrap();
        let ptr = unsafe { alloc(new_layout) } as *mut u32;
        assert!(!ptr.is_null(), "io vec allocation failed");
        unsafe {
            core::ptr::copy_nonoverlapping(old_ptr.as_ptr(), ptr, old_cap);
            dealloc(old_ptr.as_ptr().cast(), layout);
            self.heap.ptr = NonNull::new_unchecked(ptr);
            self.heap.cap = new_cap as u32;
        }
    }
}

impl Default for IoVec {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for IoVec {
    fn drop(&mut self) {
        if self.is_heap() {
            let (ptr, cap) = unsafe { (self.heap.ptr, self.heap.cap) };
            let layout = Layout::array::<u32>(cap as usize).unwrap();
            unsafe { dealloc(ptr.as_ptr().cast(), layout) };
        }
    }
}

pub struct IoVecIter<'a> {
    vec: &'a IoVec,
    i: usize,
    len: usize,
}

impl Iterator for IoVecIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.i < self.len {
            let value = self.vec.slot_at(self.i);
            self.i += 1;
            Some(value)
        } else {
            None
        }
    }
}

#[repr(C)]
pub struct IoUserData {
    pub index: u32,
    pub io_slot: u32,
}

impl From<IoUserData> for u64 {
    fn from(ud: IoUserData) -> u64 {
        unsafe { core::mem::transmute(ud) }
    }
}

impl From<u64> for IoUserData {
    fn from(raw: u64) -> Self {
        unsafe { core::mem::transmute(raw) }
    }
}

/// The joiner sentinel: no task is currently waiting on this task's join.
pub(crate) const NO_JOINER: u32 = u32::MAX;

pub struct Task {
    pub ready: bool,
    pub(crate) finished: bool,
    /// Whether [`JoinHandle::cancel`] has been called on this task. The
    /// runtime issues cancel requests for the task's in-flight io only when
    /// this is set; otherwise finished io runs to completion naturally.
    pub(crate) cancel_requested: bool,
    /// Whether `outputs[my_index]` in the runtime's output slab holds a live
    /// value: the output of the task this task joined, waiting to be read by
    /// its [`JoinFuture`]. Set by the target's finish path; cleared when the
    /// value is read or dropped.
    pub(crate) output_pending: bool,
    /// Index of the task currently awaiting this task's completion via its
    /// [`JoinHandle`], or [`NO_JOINER`]. Task indices can never equal
    /// `NO_JOINER`, so the sentinel is unambiguous.
    pub(crate) joiner: u32,
    pub io: IoVec,
    id: NonZeroU64,
}

const _: () = assert!(size_of::<Task>() == 32);

impl Task {
    /// Creates a task with a fresh unique id, `ready = false` and an empty io
    /// vec. The only way to construct a task.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let id = NEXT_TASK_ID.with(|c| {
            let id = c.get();
            c.set(id.checked_add(1).unwrap());
            id
        });
        Self {
            ready: false,
            finished: false,
            cancel_requested: false,
            output_pending: false,
            joiner: NO_JOINER,
            io: IoVec::new(),
            id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskContextError {
    InactiveRuntime,
    SlotOutOfBounds,
    TaskRemoved,
    TaskReused,
}

impl fmt::Display for TaskContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::InactiveRuntime => "TaskContext used outside the runtime it belongs to",
            Self::SlotOutOfBounds => "TaskContext refers to an out-of-bounds slot",
            Self::TaskRemoved => "TaskContext refers to a removed task",
            Self::TaskReused => "TaskContext refers to a slot reused by a different task",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for TaskContextError {}

#[derive(Debug, Clone, Copy)]
pub struct TaskContext {
    task_index: u32,
    task_id: NonZeroU64,
}

impl TaskContext {
    pub(crate) fn new(task_index: u32, task_id: NonZeroU64) -> Self {
        Self {
            task_index,
            task_id,
        }
    }

    pub fn validate(&self) -> Result<(), TaskContextError> {
        if !runtime::is_running() {
            return Err(TaskContextError::InactiveRuntime);
        }

        runtime::with_runtime(|rt| {
            let word = self.task_index as usize / 64;
            let bit = self.task_index as usize % 64;
            if word >= rt.tasks.free.len() {
                return Err(TaskContextError::SlotOutOfBounds);
            }
            if rt.tasks.free[word] & (1 << bit) != 0 {
                return Err(TaskContextError::TaskRemoved);
            }

            if rt.tasks.task(self.task_index).id != self.task_id {
                return Err(TaskContextError::TaskReused);
            }
            Ok(())
        })
    }

    pub fn with_task<R>(&self, f: impl FnOnce(&mut Task) -> R) -> R {
        if let Err(e) = self.validate() {
            panic!("invalid TaskContext: {e}");
        }
        runtime::with_runtime(|rt| f(rt.tasks.task_mut(self.task_index)))
    }

    pub(crate) fn with_runtime<R>(&self, f: impl FnOnce(&mut Runtime) -> R) -> R {
        if let Err(e) = self.validate() {
            panic!("invalid TaskContext: {e}");
        }
        runtime::with_runtime(f)
    }

    pub fn wake(&self) {
        if let Err(e) = self.validate() {
            panic!("invalid TaskContext: {e}");
        }
        runtime::with_runtime(|rt| rt.wakeups.push(self.task_index));
    }

    pub fn push_io(&self, entry: squeue::Entry, io_slot: u32) -> io::Result<()> {
        runtime::with_runtime(|rt| {
            let mut sq = rt.ring.submission();
            let user_data = IoUserData {
                index: self.task_index,
                io_slot,
            };
            let entry = entry.user_data(user_data.into());
            unsafe { sq.push(&entry) }.map_err(|_| io::Error::from_raw_os_error(libc::EAGAIN))
        })
    }
}

/// Whether the task identified by `(index, id)` still exists. The task only
/// counts as alive until it is fully removed: its future has finished and all
/// of its io has been recycled.
pub(crate) fn task_alive(index: u32, id: NonZeroU64) -> bool {
    runtime::with_runtime(|rt| rt.tasks.is_occupied(index) && rt.tasks.task(index).id == id)
}

/// A handle to a spawned task. A task's in-flight io is not cancelled when it
/// finishes; it runs to completion in the background unless
/// [`cancel`](Self::cancel) is called on the handle. The handle is generic
/// over the task's output type `R`; [`join`](Self::join) yields `Option<R>`.
#[derive(Debug)]
pub struct JoinHandle<R> {
    target: TaskContext,
    _marker: core::marker::PhantomData<R>,
}

impl<R> JoinHandle<R> {
    pub(crate) fn new(target: TaskContext) -> Self {
        Self {
            target,
            _marker: core::marker::PhantomData,
        }
    }

    /// Returns a future that resolves once the task has fully finished: its
    /// future has returned and all of its io has been recycled. The future
    /// yields the task's output, or `None` if the task was cancelled before
    /// producing one. Consumes the handle; recover it with
    /// [`JoinFuture::into_handle`] to wait later instead.
    pub fn join(self) -> JoinFuture<R> {
        JoinFuture {
            handle: Some(self),
            me: None,
            registered: false,
        }
    }

    /// Whether the task has fully finished and been removed. Panics outside an
    /// active runtime, like the other runtime-facing methods.
    pub fn is_finished(&self) -> bool {
        !task_alive(self.target.task_index, self.target.task_id)
    }

    /// Stops the task from running further and cancels its in-flight io with
    /// `IORING_OP_ASYNC_CANCEL` requests. A task that has already finished is
    /// left alone: its io completes naturally. Returns `false` if the task is
    /// already gone.
    pub fn cancel(&self) -> bool {
        if !task_alive(self.target.task_index, self.target.task_id) {
            return false;
        }
        runtime::with_runtime(|rt| {
            let task = rt.tasks.task_mut(self.target.task_index);
            if task.finished {
                return true;
            }
            task.finished = true;
            task.cancel_requested = true;
            rt.wakeups.push(self.target.task_index);
            true
        })
    }
}

/// A future returned by [`JoinHandle::join`]; resolves once the target task
/// has fully finished and been removed, yielding its output as `Option<R>`
/// (`None` if the task was cancelled before producing one).
pub struct JoinFuture<R> {
    handle: Option<JoinHandle<R>>,
    /// This future's own task index, captured at registration. Needed to read
    /// the output slot once the target is gone and to unregister on drop.
    me: Option<u32>,
    registered: bool,
}

impl<R> JoinFuture<R> {
    /// Recovers the [`JoinHandle`] so the wait can be resumed later with
    /// another [`JoinHandle::join`]. Unregisters this future's waiter first,
    /// so a later join can register again.
    pub fn into_handle(mut self) -> JoinHandle<R> {
        if let (Some(me), true) = (self.me, self.registered) {
            self.unregister(me);
            self.registered = false;
        }
        self.handle.take().expect("handle already taken")
    }

    fn unregister(&self, me: u32) {
        let target = self.handle.as_ref().expect("handle present").target;
        if task_alive(target.task_index, target.task_id) {
            runtime::with_runtime(|rt| {
                let task = rt.tasks.task_mut(target.task_index);
                if task.joiner == me {
                    task.joiner = NO_JOINER;
                }
            });
        }
    }
}

impl<R> Drop for JoinFuture<R> {
    fn drop(&mut self) {
        // Unlink from the target on drop: otherwise a removed joiner leaves a
        // stale index in `target.joiner`, and a later finish of the target
        // would write its output into a slot that may have been reused.
        if let (Some(me), true) = (self.me, self.registered) {
            self.unregister(me);
        }
    }
}

/// `JoinFuture` holds no pinned state: it only stores the handle, its own
/// index and a flag, and the output is returned by value.
impl<R> Unpin for JoinFuture<R> {}

impl<R: 'static> Future for JoinFuture<R> {
    type Output = Option<R>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let target = this.handle.as_ref().expect("handle present").target;
        if !task_alive(target.task_index, target.task_id) {
            return Poll::Ready(this.me.and_then(|me| {
                runtime::with_runtime(|rt| {
                    if rt.tasks.task(me).output_pending {
                        Some(rt.tasks.take_output::<R>(me))
                    } else {
                        None
                    }
                })
            }));
        }
        if !this.registered {
            this.registered = true;
            if let Some(waiter) = runtime::current_task_index() {
                runtime::with_runtime(|rt| {
                    let task = rt.tasks.task_mut(target.task_index);
                    assert!(task.joiner == NO_JOINER, "task already has a joiner");
                    task.joiner = waiter;
                });
                this.me = Some(waiter);
            }
        }
        Poll::Pending
    }
}

pub struct TaskSlab {
    tasks: Box<[MaybeUninit<Task>]>,
    futures: NonNull<u8>,
    future_type_id: TypeId,
    outputs: NonNull<u8>,
    output_type_id: TypeId,
    free: Box<[u64]>,
}

impl TaskSlab {
    pub(crate) fn new<F, R>(capacity: u32) -> Self
    where
        F: Future<Output = R> + 'static,
        R: 'static,
    {
        let cap = capacity as usize;
        let mut tasks = Vec::with_capacity(cap);
        tasks.resize_with(cap, MaybeUninit::uninit);
        let words = cap.div_ceil(64);
        let free = vec![u64::MAX; words].into_boxed_slice();

        let layout = Layout::array::<F>(cap).unwrap();
        let futures = unsafe { alloc(layout) };
        let futures = NonNull::new(futures).expect("future allocation failed");

        // Zero-sized `R` gets one byte per slot so the slot pointers stay
        // within an allocation; reads and writes of a ZST are no-ops that
        // produce the canonical value.
        let array_layout = Layout::array::<R>(cap).unwrap();
        let output_layout =
            Layout::from_size_align(array_layout.size().max(1), array_layout.align()).unwrap();
        let outputs = unsafe { alloc(output_layout) };
        let outputs = NonNull::new(outputs).expect("output allocation failed");

        Self {
            tasks: tasks.into_boxed_slice(),
            futures,
            future_type_id: TypeId::of::<F>(),
            outputs,
            output_type_id: TypeId::of::<R>(),
            free,
        }
    }

    pub fn is_occupied(&self, index: u32) -> bool {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        word < self.free.len() && self.free[word] & (1 << bit) == 0
    }

    /// Claims a free slot, clearing its bit, and returns its index. The slot
    /// is not initialized yet: the caller must populate it with
    /// [`Self::insert`] (or the private init methods). Private because handing
    /// out an uninitialized slot is unsafe: reads and teardown assume a clear
    /// bit means a fully initialized task.
    fn reserve(&mut self) -> Option<u32> {
        for (word_idx, word) in self.free.iter().enumerate() {
            if *word != 0 {
                let bit = word.trailing_zeros();
                let index = word_idx as u32 * 64 + bit;
                if index as usize >= self.tasks.len() || index == NO_JOINER {
                    // Out of range or the joiner sentinel: treat the slab as
                    // full rather than hand out an ambiguous index.
                    return None;
                }
                self.free[word_idx] &= !(1 << bit);
                return Some(index);
            }
        }
        None
    }

    /// Claims a free slot and initializes it with a task and a future in one
    /// call, so a clear free bit always means a fully initialized slot. This
    /// is the only safe way to create a task. Returns the new slot's index, or
    /// `None` if the slab is full.
    ///
    /// The future is built from the slot's [`TaskContext`], which only exists
    /// once the slot is claimed. `make_fut` must not panic: it runs after the
    /// slot has been claimed but before its future is written, and a panic
    /// would leave a partially initialized slot that teardown cannot drop
    /// safely.
    pub fn insert<F, R, S>(&mut self, task: Task, make_fut: S) -> Option<u32>
    where
        F: Future<Output = R> + 'static,
        R: 'static,
        S: FnOnce(TaskContext) -> F,
    {
        let index = self.reserve()?;
        unsafe {
            self.init_task(index, task);
            self.init_future(index, make_fut(self.context_for(index)));
        }
        Some(index)
    }

    fn assert_in_bounds(&self, index: u32) {
        assert!(
            (index as usize) < self.tasks.len(),
            "task slab: index {index} is out of bounds"
        );
    }

    fn assert_occupied(&self, index: u32) {
        assert!(
            self.is_occupied(index),
            "task slab: slot {index} is not initialized"
        );
    }

    /// Returns the task at `index`. Panics if `index` is out of bounds or the
    /// slot is not occupied.
    pub fn task(&self, index: u32) -> &Task {
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        unsafe { self.tasks[index as usize].assume_init_ref() }
    }

    /// Returns the task at `index` mutably. Panics if `index` is out of bounds
    /// or the slot is not occupied.
    pub fn task_mut(&mut self, index: u32) -> &mut Task {
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        unsafe { self.tasks[index as usize].assume_init_mut() }
    }

    /// Builds a [`TaskContext`] for the task at `index`. The context carries
    /// the task's unique id so it stays valid against later slot reuse.
    pub fn context_for(&self, index: u32) -> TaskContext {
        assert!(
            self.is_occupied(index),
            "cannot build a context for an uninitialized slot"
        );
        let task_id = self.task(index).id;
        TaskContext::new(index, task_id)
    }

    /// Returns a pointer to the byte slot backing future `index`. The slot is
    /// only valid for the `F` the slab was created for.
    fn future_slot<F>(&self, index: u32) -> *mut MaybeUninit<u8> {
        let base = self.futures.as_ptr().cast::<F>();
        unsafe { base.add(index as usize) as _ }
    }

    /// Returns a pointer to the byte slot backing output `index`. The slot is
    /// only valid for the `R` the slab was created for.
    fn output_slot<R>(&self, index: u32) -> *mut MaybeUninit<u8> {
        let base = self.outputs.as_ptr().cast::<R>();
        unsafe { base.add(index as usize) as _ }
    }

    /// Returns the future at `index` mutably. Panics unless `F` matches the
    /// type the slab was created for and the slot is in bounds and occupied.
    pub fn future_mut<F: 'static>(&mut self, index: u32) -> &mut F {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        unsafe { &mut *(self.future_slot::<F>(index) as *mut F) }
    }

    /// # Safety
    /// `index` must be an in-bounds slot that was claimed with
    /// [`Self::insert`] and whose task has not already been initialized.
    /// Initializing a slot a second time overwrites a live task without
    /// dropping it: its in-flight io is no longer tracked by the slot, and a
    /// stale CQE landing on a recycled slot can corrupt the runtime's io and
    /// buffer state.
    unsafe fn init_task(&mut self, index: u32, task: Task) {
        self.assert_in_bounds(index);
        self.tasks[index as usize] = MaybeUninit::new(task);
    }

    /// # Safety
    /// `index` must be an in-bounds slot whose task has already been
    /// initialized (so the slot was claimed via [`Self::insert`]), and whose
    /// future has not already been written. `F` must match the type the slab
    /// was created for. Initializing a second time leaks the previous future
    /// and can leave the slot in a state the runtime does not expect.
    unsafe fn init_future<F: 'static>(&mut self, index: u32, future: F) {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        self.assert_in_bounds(index);
        unsafe { (self.future_slot::<F>(index) as *mut F).write(future) };
    }

    /// Returns a raw pointer to the task at `index`. Panics if `index` is out
    /// of bounds or the slot is not occupied. The pointer stays valid while
    /// the task stays in its slot.
    pub fn task_ptr(&mut self, index: u32) -> *mut Task {
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        self.tasks[index as usize].as_mut_ptr()
    }

    /// Returns a raw pointer to the future at `index`. Panics unless `F`
    /// matches the type the slab was created for and the slot is in bounds and
    /// occupied.
    pub fn future_ptr<F: 'static>(&mut self, index: u32) -> *mut F {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        self.future_slot::<F>(index) as *mut F
    }

    /// Stores a finished task's output into the slot its joiner holds and
    /// marks it pending. `R` must match the type the slab was created for, the
    /// joiner slot must be in bounds and occupied, and it must not already hold
    /// an unconsumed output.
    pub fn init_output<R: 'static>(&mut self, index: u32, value: R) {
        assert_eq!(TypeId::of::<R>(), self.output_type_id);
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        assert!(
            !unsafe { self.tasks[index as usize].assume_init_ref() }.output_pending,
            "task slab: slot {index} already holds an unconsumed output"
        );
        unsafe { (self.output_slot::<R>(index) as *mut R).write(value) };
        self.task_mut(index).output_pending = true;
    }

    /// Reads and removes the output stored in slot `index`, marking it
    /// consumed. `R` must match the type the slab was created for, the slot
    /// must be in bounds and occupied, and it must hold an unconsumed output.
    pub fn take_output<R: 'static>(&mut self, index: u32) -> R {
        assert_eq!(TypeId::of::<R>(), self.output_type_id);
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        assert!(
            unsafe { self.tasks[index as usize].assume_init_ref() }.output_pending,
            "task slab: slot {index} holds no unconsumed output"
        );
        let output = unsafe { (self.output_slot::<R>(index) as *mut R).read() };
        self.task_mut(index).output_pending = false;
        output
    }

    /// Removes the task at `index`, dropping its future and any unconsumed
    /// output, and frees the slot. `F` and `R` must match the types the slab
    /// was created for and the slot must be in bounds and occupied.
    pub fn remove<F: 'static, R: 'static>(&mut self, index: u32) -> Task {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        assert_eq!(TypeId::of::<R>(), self.output_type_id);
        self.assert_in_bounds(index);
        self.assert_occupied(index);
        if self.task(index).output_pending {
            unsafe { core::ptr::drop_in_place(self.output_slot::<R>(index).cast::<R>()) };
        }
        let word = index as usize / 64;
        let bit = index as usize % 64;
        self.free[word] |= 1 << bit;
        unsafe {
            (self.future_slot::<F>(index) as *mut F).drop_in_place();
            self.tasks[index as usize].assume_init_read()
        }
    }

    /// # Safety
    /// `slab` must point to a live slab that is never used again. `F` must
    /// match the type the slab was created for.
    pub unsafe fn drop_futures_raw<F: 'static>(slab: *mut TaskSlab) {
        let this = unsafe { &*slab };
        assert_eq!(TypeId::of::<F>(), this.future_type_id);
        let layout = Layout::array::<F>(this.tasks.len()).unwrap();
        for index in 0..this.tasks.len() as u32 {
            if this.is_occupied(index) {
                unsafe { core::ptr::drop_in_place(this.future_slot::<F>(index).cast::<F>()) };
            }
        }
        if layout.size() > 0 {
            unsafe { dealloc(this.futures.as_ptr(), layout) };
        }
    }

    /// # Safety
    /// `slab` must point to a live slab that is never used again. `R` must
    /// match the type the slab was created for. Drops any unconsumed outputs
    /// still sitting in their joiners' slots and frees the output slab.
    pub unsafe fn drop_outputs_raw<R: 'static>(slab: *mut TaskSlab) {
        let this = unsafe { &*slab };
        assert_eq!(TypeId::of::<R>(), this.output_type_id);
        let array_layout = Layout::array::<R>(this.tasks.len()).unwrap();
        for index in 0..this.tasks.len() as u32 {
            if this.is_occupied(index) && this.task(index).output_pending {
                unsafe { core::ptr::drop_in_place(this.output_slot::<R>(index).cast::<R>()) };
            }
        }
        let layout =
            Layout::from_size_align(array_layout.size().max(1), array_layout.align()).unwrap();
        unsafe { dealloc(this.outputs.as_ptr(), layout) };
    }
}

#[cfg(test)]
mod tests {
    use core::future::Ready;

    use super::*;

    // ── TaskSlab ──────────────────────────────────────────────────────

    #[test]
    fn slab_new_large_capacity() {
        let slab = TaskSlab::new::<Ready<()>, ()>(128);
        assert_eq!(slab.tasks.len(), 128);
        assert_eq!(slab.free.len(), 2); // 128 / 64 = 2 words
        assert_eq!(slab.free[0], u64::MAX);
        assert_eq!(slab.free[1], u64::MAX);
    }

    #[test]
    fn reserve_sequential() {
        let mut slab = TaskSlab::new::<Ready<()>, ()>(10);
        assert_eq!(slab.reserve(), Some(0));
        assert_eq!(slab.reserve(), Some(1));
        assert_eq!(slab.reserve(), Some(2));
    }

    #[test]
    fn reserve_exhaustion() {
        let mut slab = TaskSlab::new::<Ready<()>, ()>(3);
        assert_eq!(slab.reserve(), Some(0));
        assert_eq!(slab.reserve(), Some(1));
        assert_eq!(slab.reserve(), Some(2));
        assert_eq!(slab.reserve(), None);
    }

    #[test]
    fn init_and_retrieve_task() {
        let mut slab = TaskSlab::new::<Ready<()>, ()>(10);
        let mut task = Task::new();
        task.ready = true;
        let idx = slab.insert(task, |_| core::future::ready(())).unwrap();

        let ptr = slab.task_ptr(idx);
        unsafe { assert!((*ptr).ready) };
    }

    #[test]
    fn is_occupied_initially_false() {
        let slab = TaskSlab::new::<Ready<()>, ()>(10);
        assert!(!slab.is_occupied(0));
    }

    #[test]
    fn is_occupied_after_init() {
        let mut slab = TaskSlab::new::<Ready<()>, ()>(10);
        let mut task = Task::new();
        task.ready = true;
        let idx = slab.insert(task, |_| core::future::ready(())).unwrap();
        assert!(slab.is_occupied(idx));
    }

    #[test]
    fn remove_returns_values_and_frees_slot() {
        let mut slab = TaskSlab::new::<Ready<()>, ()>(10);
        let mut task = Task::new();
        task.ready = true;
        let idx = slab.insert(task, |_| core::future::ready(())).unwrap();

        let t = slab.remove::<Ready<()>, ()>(idx);
        assert!(t.ready);
        // future is Ready<()>, dropping it after taking is fine

        // Slot should now be free again
        assert!(!slab.is_occupied(idx));
        let next_idx = slab
            .insert(Task::new(), |_| core::future::ready(()))
            .unwrap();
        assert_eq!(next_idx, idx); // reused
    }

    #[test]
    fn future_type_mismatch_panics() {
        let mut slab = TaskSlab::new::<Ready<()>, ()>(10);
        let idx = slab
            .insert(Task::new(), |_| core::future::ready(()))
            .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unsafe { slab.init_future::<Ready<u8>>(idx, core::future::ready(42u8)) };
        }));
        assert!(result.is_err());
    }

    // ── output slab ───────────────────────────────────────────────────

    #[test]
    fn output_slab_roundtrip() {
        let mut slab = TaskSlab::new::<Ready<u32>, u32>(10);
        let mut task = Task::new();
        task.ready = true;
        let idx = slab.insert(task, |_| core::future::ready(42u32)).unwrap();

        slab.init_output::<u32>(idx, 42);
        assert_eq!(slab.take_output::<u32>(idx), 42);

        // Recycle the slot; output slab is indexed the same way.
        let _ = slab.remove::<Ready<u32>, u32>(idx);
        let next = slab
            .insert(Task::new(), |_| core::future::ready(42u32))
            .unwrap();
        assert_eq!(next, idx);
    }

    struct DropCounter(std::rc::Rc<std::cell::Cell<usize>>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn remove_drops_pending_output() {
        let output_drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let future_drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut slab = TaskSlab::new::<Ready<DropCounter>, DropCounter>(10);
        let mut task = Task::new();
        task.ready = true;
        let idx = slab
            .insert(task, |_| {
                core::future::ready(DropCounter(future_drops.clone()))
            })
            .unwrap();
        slab.init_output::<DropCounter>(idx, DropCounter(output_drops.clone()));

        let _ = slab.remove::<Ready<DropCounter>, DropCounter>(idx);
        assert_eq!(output_drops.get(), 1);
        assert_eq!(future_drops.get(), 1);
    }

    #[test]
    fn remove_keeps_consumed_output() {
        let output_drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let future_drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut slab = TaskSlab::new::<Ready<DropCounter>, DropCounter>(10);
        let mut task = Task::new();
        task.ready = true;
        let idx = slab
            .insert(task, |_| {
                core::future::ready(DropCounter(future_drops.clone()))
            })
            .unwrap();
        slab.init_output::<DropCounter>(idx, DropCounter(output_drops.clone()));

        // The joiner consumed the output: flag cleared, removal drops nothing.
        slab.task_mut(idx).output_pending = false;
        let _ = slab.remove::<Ready<DropCounter>, DropCounter>(idx);
        assert_eq!(output_drops.get(), 0);
        assert_eq!(future_drops.get(), 1);
    }

    #[test]
    fn drop_outputs_raw_drops_unconsumed_outputs() {
        let output_drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let future_drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut slab = TaskSlab::new::<Ready<DropCounter>, DropCounter>(10);
        let mut task = Task::new();
        task.ready = true;
        let idx = slab
            .insert(task, |_| {
                core::future::ready(DropCounter(future_drops.clone()))
            })
            .unwrap();
        slab.init_output::<DropCounter>(idx, DropCounter(output_drops.clone()));

        unsafe { TaskSlab::drop_outputs_raw::<DropCounter>(&mut slab) };
        assert_eq!(output_drops.get(), 1);
        assert_eq!(future_drops.get(), 0);
    }

    // ── IoVec ─────────────────────────────────────────────────────────

    #[test]
    fn io_vec_new_is_empty_inline() {
        let v = IoVec::new();
        assert!(v.is_empty());
        assert!(!v.is_heap());
    }

    #[test]
    fn io_vec_stays_inline_up_to_three() {
        let mut v = IoVec::new();
        v.push(7);
        v.push(8);
        v.push(9);
        assert!(!v.is_heap());
        assert_eq!(v.len(), 3);
        assert_eq!(v.iter().collect::<Vec<_>>(), vec![7, 8, 9]);
    }

    #[test]
    fn io_vec_grows_to_heap_on_fourth_push() {
        let mut v = IoVec::new();
        for i in 0..4 {
            v.push(i);
        }
        assert!(v.is_heap());
        assert_eq!(v.len(), 4);
        assert_eq!(v.iter().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn io_vec_grows_past_initial_heap_capacity() {
        let mut v = IoVec::new();
        for i in 0..20 {
            v.push(i);
        }
        assert!(v.is_heap());
        assert_eq!(v.len(), 20);
        assert_eq!(v.iter().collect::<Vec<_>>(), (0..20).collect::<Vec<_>>());
    }

    #[test]
    fn io_vec_remove_value_missing() {
        let mut v = IoVec::new();
        v.push(1);
        v.push(2);
        assert!(!v.remove_value(3));
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn io_vec_remove_value_inline() {
        let mut v = IoVec::new();
        v.push(1);
        v.push(2);
        v.push(3);
        assert!(v.remove_value(2));
        assert_eq!(v.iter().collect::<Vec<_>>(), vec![1, 3]);
        assert!(!v.is_heap());
    }

    #[test]
    fn io_vec_remove_value_heap() {
        let mut v = IoVec::new();
        for i in 0..10 {
            v.push(i);
        }
        assert!(v.remove_value(3));
        assert_eq!(v.len(), 9);
        let mut rest: Vec<u32> = v.iter().collect();
        rest.sort();
        assert_eq!(rest, (0..10).filter(|&i| i != 3).collect::<Vec<_>>());
    }

    #[test]
    fn io_vec_heap_allocation_is_freed_on_drop() {
        // Drop a heap-backed vec; the test harness would catch a leak or
        // double-free.
        let mut v = IoVec::new();
        for i in 0..100 {
            v.push(i);
        }
        assert!(v.is_heap());
    }
}
