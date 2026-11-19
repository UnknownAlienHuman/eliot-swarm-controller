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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn back_and_forward_walk_the_visits_like_a_browser() {
        let mut history = History::default();
        for place in ["home", "a1", "a2"] {
            history.visit(place);
        }
        assert!(!history.can_forward());
        assert_eq!(history.back(), Some("a1"));
        assert_eq!(history.back(), Some("home"));
        assert_eq!(history.back(), None);
        assert_eq!(history.forward(), Some("a1"));
        // A new place drops what was ahead.
        history.visit("settings");
        assert!(!history.can_forward());
        assert_eq!(history.back(), Some("a1"));
        assert_eq!(history.back(), Some("home"));
    }

    #[test]
    fn the_same_place_again_is_no_visit() {
        let mut history = History::default();
        history.visit("home");
        history.visit("a1");
        history.visit("a1");
        assert_eq!(history.back(), Some("home"));
        assert!(!history.can_back());
        // Arriving where `back` went is not a new visit either.
        history.visit("home");
        assert_eq!(history.forward(), Some("a1"));
    }

    #[test]
    fn depth_is_bounded() {
        let mut history = History::default();
        for place in 0..(DEPTH + 10) {
            history.visit(place);
        }
        let mut steps = 0;
        while history.back().is_some() {
            steps += 1;
        }
        assert_eq!(steps, DEPTH);
    }

    #[test]
    fn a_forgotten_place_is_skipped() {
        let mut history = History::default();
        for place in ["home", "a1", "home", "a2"] {
            history.visit(place);
        }
        history.forget(&"a1");
        assert_eq!(history.back(), Some("home"));
        assert_eq!(history.back(), None);
    }
}
