use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[derive(Debug)]
pub struct WaitResult {
    is_leader: bool,
}

impl WaitResult {
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

pub struct Barrier(Arc<Shared>);

impl Barrier {
    pub fn new() -> Self {
        Self(Arc::new(Shared {
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
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::atomic::Ordering::*;
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
    fn adjust_pani() {
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
