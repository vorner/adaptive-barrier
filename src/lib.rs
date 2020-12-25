//! A synchronization barrier that adapts to the number of subscribing threads.
//!
//! This works in the same way as the [`Barrier`][std::sync::Barrier], except that it can adapt to
//! changing number of subscribing threads. This is convenient if one wants to add or remove worker
//! threads during the lifetime of an algorithm. However, the main use case is dealing with
//! panics.
//!
//! In particular, in case one of the synchronized threads panics, the others will never reach the
//! number needed to get unblocked (because the panicked one is missing) and the whole thing
//! deadlocks. The [`Barrier`] in here will notice the thread is missing and adjust the expected
//! numbers. It is on the code around to deal with the panicking thread and make some sense of it
//! (or abort), but this will at least give it the *chance* to do so.
//!
//! It can also help tests using threads recover and report from failures instead of locking up.
#![doc(test(attr(deny(warnings))))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

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
}

impl Inner {
    fn check_release(&mut self) -> bool {
        if self.waiting >= self.active {
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
}

/// A Barrier to synchronize multiple threads.
///
/// Multiple threads can meet on a single barrier to start a computation in concert. This works
/// much like the [`Barrier`][std::sync::Barrier] from the standard library.
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
/// # use adaptive_barrier::Barrier;
///
/// let barrier = Barrier::new();
/// let mut threads = Vec::new();
/// for _ in 0..4 {
///     // Each thread gets its own clone of the barrier. They are tied together, not independent.
///     let mut barrier = barrier.clone();
///     let thread = thread::spawn(move || {
///         // Wait to start everything at the same time
///         barrier.wait();
///
///         // ... Do some work that needs to start synchronously ...
///         // Now, if this part panics, it will *not* deadlock, it'll wait for all the living
///         // threads.
///
///         // Wait for all threads to finish
///         if barrier.wait().is_leader() {
///             // Pick one thread to consolidate the results here
///             // If some threads panicked, this needs to deal with the missing one somehow (or
///             // propagate the panic).
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
    pub fn new() -> Self {
        Barrier(Arc::new(Shared {
            inner: Mutex::new(Inner {
                active: 1, // this thread
                waiting: 0,
                gen: 0,
                leader: false,
            }),
            condvar: Condvar::new(),
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
    pub fn wait(&mut self) -> WaitResult {
        let mut lock = self.0.inner.lock().unwrap();
        lock.waiting += 1;
        let gen = lock.gen;
        self.check_release(&mut lock);
        while gen == lock.gen {
            lock = self.0.condvar.wait(lock).unwrap();
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
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread::{self, sleep};
    use std::time::Duration;

    use super::*;

    /// When we have just one instance, it doesn't wait.
    #[test]
    fn single() {
        let mut bar = Barrier::new();
        assert!(bar.wait().is_leader());
    }

    /// Check the barriers wait for each other.
    #[test]
    fn dispatch() {
        let mut bar = Barrier::new();
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
        let mut bar = Barrier::new();
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
        let mut bar = Barrier::new();
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
        let mut bar = Barrier::new();
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
        let bar = Barrier::new();
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
}
