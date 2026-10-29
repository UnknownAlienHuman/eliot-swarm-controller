//! Explicit poison recovery for synchronous runtime mutexes.

use std::sync::{Mutex, MutexGuard, PoisonError};

pub trait LockExt<T: ?Sized> {
    /// Recover the guard without clearing the mutex's poison flag.
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> LockExt<T> for Mutex<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{panic::catch_unwind, sync::TryLockError};

    #[test]
    fn recovered_guard_holds_lock_and_preserves_poison() {
        let mutex = Mutex::new(1);
        assert!(
            catch_unwind(|| {
                let mut guard = mutex.lock().unwrap();
                *guard = 2;
                panic!("poison fixture");
            })
            .is_err()
        );
        {
            let mut guard = mutex.lock_recover();
            assert_eq!(*guard, 2);
            *guard = 3;
            assert!(matches!(mutex.try_lock(), Err(TryLockError::WouldBlock)));
        }
        assert!(mutex.is_poisoned());
        match mutex.try_lock() {
            Err(TryLockError::Poisoned(error)) => assert_eq!(*error.into_inner(), 3),
            _ => panic!("released mutex must retain its poison flag"),
        }
    }
}
