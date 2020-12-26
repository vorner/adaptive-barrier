//! A synchronization barrier that adapts to the number of subscribing threads.
//!
//! This has the same goal as the [`std::sync::Barrier`], but it handles runtime additions
//! or removals of thread subscriptions ‒ the number of threads waiting for the barrier can change
//! (even while some threads are already waiting).
//!
//! It can be convenient if your algorithm changes the number of working threads during lifetime.
//! You don't need a different barrier for different phases of the algorithm.
//!
//! But most importantly, the [`Barrier`] is robust in face of panics.
//!
//! # Problems with panics and the [`std::sync::Barrier`]
//!
//! If we have a barrier that was set up for `n` threads, some of the threads park on it and wait
//! for the rest to finish, but one of the other threads has a bug and panics, the already parked
//! threads will never get a chance to continue and the whole algorithm deadlocks. This is usually
//! worse than propagating the panic and cleaning up the whole algorithm or even shutting down the
//! whole application, because then something can recover by restarting it. If the application
//! deadlocks in the computation phase, but otherwise looks healthy, it will never recover.
//!
//! This makes applications less robust and makes tests which use barriers very annoying and
//! fragile to write.
//!
//! Our [`Barrier`] watches the number of subscribed threads (by counting the number of its own
//! clones, unlike the standard barrier, this one can and need to be cloned for each thread). If a
//! thread disappears (or is added), the expectations are adjusted.
//!
//! It also has a mode in which it'll get poisoned and propagate the panic to the rest of the
//! group.
#![doc(test(attr(deny(warnings))))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem;
use std::panic::UnwindSafe;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

/// What to do if a [`Barrier`] is destroyed during a panic.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PanicMode {
    /// Nothing special.
    ///
    /// Just decrement the number of expected threads, just like during any normal destruction.
    Decrement,

    /// Poison the barrier.
    ///
    /// All calls to [`wait`][Barrier::wait], including the ones that are already in progress, will
    /// panic too. Once poisoned, there's no way to "unpoison" the barrier.
    ///
    /// This is useful in case a failure in one thread makes the whole group unusable (very often
    /// in tests).
    Poison,
}

/// A result after waiting.
///
/// This can be used to designate a single thread as the leader after calling
/// [`wait`][Barrier::wait].
#[derive(Debug)]
pub struct WaitResult {
    is_leader: bool,
}

impl WaitResult {
    /// Returns true for exactly one thread from a waiting group.
    ///
    /// An algorithm can use that to pick a thread between equals that'll do some singleton thing
    /// (consolidate the results, for example).
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

struct Inner {
    active: usize,
    waiting: usize,
    gen: usize,
    leader: bool,
    poisoned: bool,
}

impl Inner {
    fn check_release(&mut self) -> bool {
        if self.waiting >= self.active || self.poisoned {
            self.leader = true;
            self.gen = self.gen.wrapping_add(1);
            self.waiting = 0;
            true
        } else {
            false
        }
    }
}

struct Shared {
    inner: Mutex<Inner>,
    condvar: Condvar,
    panic_mode: PanicMode,
}

/// A Barrier to synchronize multiple threads.
///
/// Multiple threads can meet on a single barrier to synchronize a "meeting point" in a computation
/// (eg. when they need to pass results to others), much like the [`Barrier`][std::sync::Barrier]
/// from the standard library.
///
/// Unlike that, the expected number of threads waiting for the barrier is not preset in the `new`
/// call, but autodetected and adapted to at runtime.
///
/// The way this is done is by cloning the original [`Barrier`] ‒ for a group to continue after
/// wait, a [`wait`][Barrier::wait] needs to be called on each clone. This allows to add or remove
/// (even implicitly by panicking) the clones as needed.
///
/// # Examples
///
/// ```rust
/// # use std::thread;
/// # use adaptive_barrier::{Barrier, PanicMode};
///
/// let barrier = Barrier::new(PanicMode::Poison);
/// let mut threads = Vec::new();
/// for _ in 0..4 {
///     // Each thread gets its own clone of the barrier. They are tied together, not independent.
///     let mut barrier = barrier.clone();
///     let thread = thread::spawn(move || {
///         // Wait to start everything at the same time
///         barrier.wait();
///
///         // ... Do some work that needs to start synchronously ...
///         // Now, if this part panics, it will *not* deadlock, it'll unlock the others just fine
///         // and propagate the panic (see the parameter to new(..)
///
///         // Wait for all threads to finish
///         if barrier.wait().is_leader() {
///             // Pick one thread to consolidate the results here
///
///             // Note that as we don't call wait any more, if we panic here, it'll not get
///             // propagated through the barrier any more.
///         }
///     });
///     threads.push(thread);
/// }
///
/// // Watch out for the last instance here in the main/controlling thread. You can either call
/// // wait on it too, or make sure it is dropped. If you don't, others will keep waiting for it.
/// drop(barrier);
///
/// for thread in threads {
///     thread.join().expect("Propagating thread panic");
/// }
/// ```
pub struct Barrier(Arc<Shared>);

impl Barrier {
    /// Creates a new (independent) barrier.
    ///
    /// To create more handles to the same barrier, clone it.
    ///
    /// The panic mode specifies what to do if a barrier observes a panic (is dropped while
    /// panicking).
    pub fn new(panic_mode: PanicMode) -> Self {
        Barrier(Arc::new(Shared {
            inner: Mutex::new(Inner {
                active: 1, // this thread
                waiting: 0,
                gen: 0,
                leader: false,
                poisoned: false,
            }),
            condvar: Condvar::new(),
            panic_mode,
        }))
    }

    fn check_release(&self, lock: &mut MutexGuard<'_, Inner>) {
        if lock.check_release() {
            self.0.condvar.notify_all();
        }
    }

    /// Wait for all the other threads to wait too.
    ///
    /// This'll block until all threads holding clones of the same barrier call `wait`.
    ///
    /// # Panics
    ///
    /// If the barrier was created with [`PanicMode::Poison`] and some other clone of the barrier
    /// observed a panic, this'll also panic (even if it was already parked inside).
    pub fn wait(&mut self) -> WaitResult {
        let mut lock = self.0.inner.lock().unwrap();
        lock.waiting += 1;
        let gen = lock.gen;
        self.check_release(&mut lock);
        while gen == lock.gen {
            lock = self.0.condvar.wait(lock).unwrap();
        }
        if lock.poisoned {
            drop(lock); // Make sure we don't poison the mutex too
            panic!("Barrier is poisoned");
        }
        WaitResult {
            is_leader: mem::replace(&mut lock.leader, false),
        }
    }
}

impl Clone for Barrier {
    fn clone(&self) -> Self {
        let new = Arc::clone(&self.0);
        new.inner.lock().unwrap().active += 1;
        Barrier(new)
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        let mut lock = self.0.inner.lock().unwrap();
        lock.active -= 1;

        if self.0.panic_mode == PanicMode::Poison && thread::panicking() {
            lock.poisoned = true;
        }

        self.check_release(&mut lock);
    }
}

impl Debug for Barrier {
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        fmt.pad("Barrier { .. }")
    }
}

impl Default for Barrier {
    fn default() -> Self {
        Self::new(PanicMode::Decrement)
    }
}

// We deal with panics explicitly.
impl UnwindSafe for Barrier {}

#[cfg(test)]
mod tests {
    use std::panic;
    use std::sync::atomic::Ordering::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread::{self, sleep};
    use std::time::Duration;

    use super::*;

    /// When we have just one instance, it doesn't wait.
    #[test]
    fn single() {
        let mut bar = Barrier::new(PanicMode::Decrement);
        assert!(bar.wait().is_leader());
    }

    /// Check the barriers wait for each other.
    #[test]
    fn dispatch() {
        let mut bar = Barrier::new(PanicMode::Decrement);
        let waited = Arc::new(AtomicBool::new(false));
        let t = thread::spawn({
            let mut bar = bar.clone();
            let waited = Arc::clone(&waited);
            move || {
                bar.wait();
                waited.store(true, SeqCst);
                bar.wait();
            }
        });

        sleep(Duration::from_millis(50));
        assert!(!waited.load(SeqCst));
        bar.wait();
        bar.wait();
        assert!(waited.load(SeqCst));

        t.join().unwrap();
    }

    #[test]
    fn adjust_up() {
        let mut bar = Barrier::new(PanicMode::Decrement);
        let woken = Arc::new(AtomicUsize::new(0));
        let t1 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                bar.wait();
                woken.fetch_add(1, SeqCst);
                bar.wait();
            }
        });

        sleep(Duration::from_millis(50));
        assert_eq!(woken.load(SeqCst), 0);

        let t2 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                bar.wait();
                woken.fetch_add(1, SeqCst);
                bar.wait();
            }
        });

        sleep(Duration::from_millis(50));
        assert_eq!(woken.load(SeqCst), 0);

        bar.wait();
        bar.wait();
        assert_eq!(woken.load(SeqCst), 2);

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn adjust_down() {
        let mut bar = Barrier::new(PanicMode::Decrement);
        let woken = Arc::new(AtomicUsize::new(0));
        let t1 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                bar.wait();
                woken.fetch_add(1, SeqCst);
                bar.wait();
            }
        });

        let t2 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                // Only one wait, the second one will be done on only 2 copies
                bar.wait();
                woken.fetch_add(1, SeqCst);
            }
        });

        sleep(Duration::from_millis(50));
        assert_eq!(woken.load(SeqCst), 0);

        bar.wait();
        t2.join().unwrap();
        bar.wait();
        assert_eq!(woken.load(SeqCst), 2);

        t1.join().unwrap();
    }

    #[test]
    fn adjust_panic() {
        let mut bar = Barrier::new(PanicMode::Decrement);
        let woken = Arc::new(AtomicUsize::new(0));
        let t1 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                bar.wait();
                woken.fetch_add(1, SeqCst);
                bar.wait();
                woken.fetch_add(1, SeqCst);
            }
        });

        let t2 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                // Only one wait, the second one will be done on only 2 copies
                bar.wait();
                woken.fetch_add(1, SeqCst);
                panic!("We are going to panic, woohooo, the thing still adjusts");
            }
        });

        sleep(Duration::from_millis(50));
        assert_eq!(woken.load(SeqCst), 0);

        bar.wait();
        t2.join().unwrap_err();
        bar.wait();

        t1.join().unwrap();

        assert_eq!(woken.load(SeqCst), 3);
    }

    #[test]
    fn adjust_drop() {
        let bar = Barrier::new(PanicMode::Decrement);
        let woken = Arc::new(AtomicUsize::new(0));
        let t1 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                bar.wait();
                woken.fetch_add(1, SeqCst);
                bar.wait();
            }
        });

        sleep(Duration::from_millis(50));
        assert_eq!(woken.load(SeqCst), 0);

        let t2 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::clone(&woken);
            move || {
                bar.wait();
                woken.fetch_add(1, SeqCst);
                bar.wait();
            }
        });

        sleep(Duration::from_millis(50));
        assert_eq!(woken.load(SeqCst), 0);
        drop(bar);

        t1.join().unwrap();
        t2.join().unwrap();
        assert_eq!(woken.load(SeqCst), 2);
    }

    /// Poisoning of the barrier.
    #[test]
    #[cfg_attr(clippy, allow(clippy::redundant_clone))]
    fn poisoning() {
        let mut bar = Barrier::new(PanicMode::Poison);
        let woken = Arc::new(AtomicUsize::new(0));

        let t1 = thread::spawn({
            let mut bar = bar.clone();
            let woken = Arc::new(AtomicUsize::new(0));
            move || {
                bar.wait();
                woken.fetch_add(1, SeqCst);
                bar.wait();
            }
        });

        sleep(Duration::from_millis(50));
        assert_eq!(woken.load(SeqCst), 0);

        let t2 = thread::spawn({
            let bar = bar.clone();
            move || {
                // Make sure this one gets into the closure so we destroy it on the panic.
                // Not issue in practice where it would get pulled in by .wait(), but we don't have
                // one here in test.
                let _bar = bar;
                panic!("Testing a panic");
            }
        });

        // The thread 2 panics
        t2.join().unwrap_err();
        // And the panic propagates to t1, even though we still hold our copy of barrier.
        t1.join().unwrap_err();

        // Our last instance would panic too.
        panic::catch_unwind(move || bar.wait()).unwrap_err();
    }
}
