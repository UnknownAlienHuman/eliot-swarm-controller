//! Back and forward through the places the owner went, as in the Codex app
//! (and every browser): visiting a new place drops the forward list, going
//! back and forward moves along it. Pure data, so it is tested without a
//! window; the shell records the place it shows after every update and
//! goes to the place these steps return.

/// The places kept behind the current one.
const DEPTH: usize = 50;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct History<T> {
    back: Vec<T>,
    current: Option<T>,
    forward: Vec<T>,
}

impl<T> Default for History<T> {
    fn default() -> Self {
        Self {
            back: Vec::new(),
            current: None,
            forward: Vec::new(),
        }
    }
}

impl<T: Clone + PartialEq> History<T> {
    /// The owner is at `place` now. The same place again is no visit; a
    /// new one keeps the current place behind it and drops what was ahead.
    pub fn visit(&mut self, place: T) {
        if self.current.as_ref() == Some(&place) {
            return;
        }
        if let Some(current) = self.current.replace(place) {
            self.back.push(current);
            if self.back.len() > DEPTH {
                self.back.remove(0);
            }
        }
        self.forward.clear();
    }

    /// One step back: the place to show, now current.
    pub fn back(&mut self) -> Option<T> {
        let place = self.back.pop()?;
        if let Some(current) = self.current.replace(place.clone()) {
            self.forward.push(current);
        }
        Some(place)
    }

    /// One step forward: the place to show, now current.
    pub fn forward(&mut self) -> Option<T> {
        let place = self.forward.pop()?;
        if let Some(current) = self.current.replace(place.clone()) {
            self.back.push(current);
        }
        Some(place)
    }

    /// The place one step back (or forward), without moving there: a task
    /// is read from the daemon before the step is taken.
    pub fn peek(&self, forward: bool) -> Option<&T> {
        if forward {
            self.forward.last()
        } else {
            self.back.last()
        }
    }

    pub fn can_back(&self) -> bool {
        !self.back.is_empty()
    }

    pub fn can_forward(&self) -> bool {
        !self.forward.is_empty()
    }

    /// Forget `place` wherever it is kept (a deleted task), so back and
    /// forward never step onto it; the current place stays.
    pub fn forget(&mut self, place: &T) {
        self.back.retain(|kept| kept != place);
        self.forward.retain(|kept| kept != place);
        // Two equal neighbours left by the removal are one step.
        self.back.dedup();
        self.forward.dedup();
        if self.back.last() == self.current.as_ref() {
            self.back.pop();
        }
        if self.forward.last() == self.current.as_ref() {
            self.forward.pop();
        }
    }
}
