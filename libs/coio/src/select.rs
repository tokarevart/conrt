//! Concurrently poll two or three caller-owned futures, left-biased.
//!
//! [`select2`] polls both futures in order and returns as soon as one reports
//! `Ready`, handing back its output plus *both* futures so the caller keeps
//! ownership: the loser is returned for re-polling (its in-flight io and
//! internal state are preserved) and the winner is recreated by the caller.
//!
//! The branches must be `Unpin` so the returned futures can be moved out of
//! the select state machine after being polled. `async`-fn futures are `Unpin`
//! unless they hold a self-referential type across an `await`, which the io
//! futures in this crate never do.
//!
//! An all-pending select returns `Poll::Pending` without waking the task: it
//! is re-polled by the runtime when a branch's io completes (via CQE drain) or
//! an explicit wake arrives. There is no internal yield and no busy loop.

use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;

/// The output of [`select2`]: which branch won, its output, and both futures
/// returned to the caller.
///
/// `first`/`second` are `Some` only for the winning branch; the futures in
/// `a`/`b` are always returned. The winner's future has completed and should
/// be replaced by a fresh one; the loser's future is returned pending so it
/// can be re-polled without losing its in-flight io.
pub struct Select2Out<A: Future, B: Future> {
    /// The output of `a`, if it won.
    pub first: Option<A::Output>,
    /// The output of `b`, if it won.
    pub second: Option<B::Output>,
    /// The `a` future, returned for re-polling (or completed if `first` is
    /// `Some`).
    pub a: A,
    /// The `b` future, returned for re-polling (or completed if `second` is
    /// `Some`).
    pub b: B,
}

/// The future returned by [`select2`].
pub struct Select2<A, B> {
    a: Option<A>,
    b: Option<B>,
}

/// Concurrently polls `a` and `b`, returning as soon as either completes.
/// Left-biased: when both are ready, `a` wins.
pub fn select2<A: Future + Unpin, B: Future + Unpin>(a: A, b: B) -> Select2<A, B> {
    Select2 {
        a: Some(a),
        b: Some(b),
    }
}

impl<A: Future + Unpin, B: Future + Unpin> Future for Select2<A, B> {
    type Output = Select2Out<A, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(a) = this.a.as_mut() {
            match Pin::new(a).poll(cx) {
                Poll::Ready(output) => {
                    return Poll::Ready(Select2Out {
                        first: Some(output),
                        second: None,
                        a: this.a.take().expect("select2: a already taken"),
                        b: this.b.take().expect("select2: b already taken"),
                    });
                }
                Poll::Pending => {}
            }
        }

        if let Some(b) = this.b.as_mut() {
            match Pin::new(b).poll(cx) {
                Poll::Ready(output) => {
                    return Poll::Ready(Select2Out {
                        first: None,
                        second: Some(output),
                        a: this.a.take().expect("select2: a already taken"),
                        b: this.b.take().expect("select2: b already taken"),
                    });
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}

/// The output of [`select3`], mirroring [`Select2Out`] with a third branch.
pub struct Select3Out<A: Future, B: Future, C: Future> {
    /// The output of `a`, if it won.
    pub first: Option<A::Output>,
    /// The output of `b`, if it won.
    pub second: Option<B::Output>,
    /// The output of `c`, if it won.
    pub third: Option<C::Output>,
    /// The `a` future, returned for re-polling (or completed if `first` is
    /// `Some`).
    pub a: A,
    /// The `b` future, returned for re-polling (or completed if `second` is
    /// `Some`).
    pub b: B,
    /// The `c` future, returned for re-polling (or completed if `third` is
    /// `Some`).
    pub c: C,
}

/// The future returned by [`select3`].
pub struct Select3<A, B, C> {
    a: Option<A>,
    b: Option<B>,
    c: Option<C>,
}

/// Concurrently polls `a`, `b` and `c`, returning as soon as one completes.
/// Left-biased: earlier branches win ties.
pub fn select3<A: Future + Unpin, B: Future + Unpin, C: Future + Unpin>(
    a: A,
    b: B,
    c: C,
) -> Select3<A, B, C> {
    Select3 {
        a: Some(a),
        b: Some(b),
        c: Some(c),
    }
}

impl<A: Future + Unpin, B: Future + Unpin, C: Future + Unpin> Future for Select3<A, B, C> {
    type Output = Select3Out<A, B, C>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(a) = this.a.as_mut() {
            match Pin::new(a).poll(cx) {
                Poll::Ready(output) => {
                    return Poll::Ready(Select3Out {
                        first: Some(output),
                        second: None,
                        third: None,
                        a: this.a.take().expect("select3: a already taken"),
                        b: this.b.take().expect("select3: b already taken"),
                        c: this.c.take().expect("select3: c already taken"),
                    });
                }
                Poll::Pending => {}
            }
        }

        if let Some(b) = this.b.as_mut() {
            match Pin::new(b).poll(cx) {
                Poll::Ready(output) => {
                    return Poll::Ready(Select3Out {
                        first: None,
                        second: Some(output),
                        third: None,
                        a: this.a.take().expect("select3: a already taken"),
                        b: this.b.take().expect("select3: b already taken"),
                        c: this.c.take().expect("select3: c already taken"),
                    });
                }
                Poll::Pending => {}
            }
        }

        if let Some(c) = this.c.as_mut() {
            match Pin::new(c).poll(cx) {
                Poll::Ready(output) => {
                    return Poll::Ready(Select3Out {
                        first: None,
                        second: None,
                        third: Some(output),
                        a: this.a.take().expect("select3: a already taken"),
                        b: this.b.take().expect("select3: b already taken"),
                        c: this.c.take().expect("select3: c already taken"),
                    });
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select2_left_bias_when_both_ready() {
        let s = select2(core::future::ready(1), core::future::ready(2));
        let mut s = core::pin::pin!(s);
        let mut cx = Context::from_waker(core::task::Waker::noop());
        match s.as_mut().poll(&mut cx) {
            Poll::Ready(out) => {
                assert_eq!(out.first, Some(1));
                assert_eq!(out.second, None);
            }
            Poll::Pending => panic!("both ready: select2 must resolve immediately"),
        }
    }

    #[test]
    fn select2_second_wins_when_first_pending() {
        let s = select2(core::future::pending::<i32>(), core::future::ready(2));
        let mut s = core::pin::pin!(s);
        let mut cx = Context::from_waker(core::task::Waker::noop());
        match s.as_mut().poll(&mut cx) {
            Poll::Ready(out) => {
                assert_eq!(out.first, None);
                assert_eq!(out.second, Some(2));
            }
            Poll::Pending => panic!("b ready: select2 must resolve immediately"),
        }
    }

    #[test]
    fn select2_both_pending_returns_pending() {
        let s = select2(core::future::pending::<()>(), core::future::pending::<()>());
        let mut s = core::pin::pin!(s);
        let mut cx = Context::from_waker(core::task::Waker::noop());
        assert!(matches!(s.as_mut().poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn select3_left_bias_and_pending() {
        let s = select3(
            core::future::ready(1),
            core::future::ready(2),
            core::future::pending::<i32>(),
        );
        let mut s = core::pin::pin!(s);
        let mut cx = Context::from_waker(core::task::Waker::noop());
        match s.as_mut().poll(&mut cx) {
            Poll::Ready(out) => {
                assert_eq!(out.first, Some(1));
                assert_eq!(out.second, None);
                assert_eq!(out.third, None);
            }
            Poll::Pending => panic!("a and b ready: select3 must resolve immediately"),
        }
    }
}
