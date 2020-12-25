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
        lock.check_release();
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
