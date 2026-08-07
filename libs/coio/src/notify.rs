//! A one-shot, single-waiter notification carrying a value, for coordinating
//! tasks on the same runtime.
//!
//! [`Notify`] is `Rc`-based and therefore neither `Send` nor `Sync`: it is
//! meant to be shared between tasks of one runtime (typically through an
//! `Rc<RefCell<DaemonState>>`), not across threads.

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;

use crate::task::TaskContext;

/// A one-shot, single-waiter notification carrying a value.
///
/// [`Notify::notify`] delivers a value to a task blocked on [`Notify::wait`],
/// or stores it for the next `wait` if none is registered yet. A value
/// delivered and not yet consumed is overwritten by a later [`Notify::notify`].
pub struct Notify<T> {
    inner: Rc<RefCell<Inner<T>>>,
}

struct Inner<T> {
    value: Option<T>,
    waiter: Option<TaskContext>,
}

impl<T> Clone for Notify<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T> Notify<T> {
    /// Creates an empty notification.
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {
                value: None,
                waiter: None,
            })),
        }
    }

    /// Delivers `value`, waking the registered waiter if any. Safe to call
    /// before any [`Notify::wait`] (the value is stored) and after a waiter
    /// dropped its `Wait` future (a closed session: the value is just stored,
    /// never a panic). Repeated notifications overwrite an unconsumed value.
    pub fn notify(&self, value: T) {
        let mut inner = self.inner.borrow_mut();
        inner.value = Some(value);
        if let Some(waiter) = inner.waiter {
            // Keep the waiter registered so only it can consume the value; a
            // second concurrent wait panics instead of stealing it.
            drop(inner);
            waiter.wake();
        }
    }

    /// Returns a future that completes with the value delivered by
    /// [`Notify::notify`], registering `ctx` as the sole waiter. A second
    /// concurrent wait on the same `Notify` panics. Dropping the returned
    /// future without completing unregisters the waiter, so a later `notify`
    /// is a no-op.
    pub fn wait(&self, ctx: TaskContext) -> Wait<T> {
        Wait {
            inner: Rc::clone(&self.inner),
            ctx,
            registered: false,
        }
    }
}

impl<T> Default for Notify<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// The future returned by [`Notify::wait`].
pub struct Wait<T> {
    inner: Rc<RefCell<Inner<T>>>,
    ctx: TaskContext,
    registered: bool,
}

impl<T> core::future::Future for Wait<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut core::task::Context<'_>) -> core::task::Poll<T> {
        use core::task::Poll;

        let this = self.get_mut();
        if !this.registered {
            this.registered = true;
            let mut inner = this.inner.borrow_mut();
            if inner.waiter.is_some() {
                panic!("Notify::wait: a waiter is already registered on this Notify");
            }
            if let Some(value) = inner.value.take() {
                return Poll::Ready(value);
            }
            inner.waiter = Some(this.ctx);
            Poll::Pending
        } else {
            let mut inner = this.inner.borrow_mut();
            if let Some(value) = inner.value.take() {
                inner.waiter = None;
                Poll::Ready(value)
            } else {
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for Wait<T> {
    fn drop(&mut self) {
        // At most one waiter can be registered, and this future registered at
        // most once, so clearing unconditionally is safe.
        if self.registered {
            self.inner.borrow_mut().waiter = None;
        }
    }
}
