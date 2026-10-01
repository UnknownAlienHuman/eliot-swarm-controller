//! Byte ranges flagged for replacement, and how overlapping flags collapse.
//!
//! Every layer reports *where* it found something rather than rewriting the
//! string itself. Two layers routinely flag overlapping spans — a credentialed
//! URI whose password is also high-entropy, a vendor rule inside a connection
//! string — and replacing eagerly would corrupt offsets for whichever layer ran
//! second. Collecting spans and applying them once, after a merge, is what makes
//! the layer set order-independent.

use crate::Category;

/// One flagged byte range, and which layer flagged it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Region {
    pub start: usize,
    pub end: usize,
    pub category: Category,
}

impl Region {
    pub(crate) fn new(start: usize, end: usize, category: Category) -> Self {
        Self { start, end, category }
    }
}

/// Sort, merge overlaps, and splice `placeholder` in over every merged span.
///
/// Returns the rewritten string and the per-category tally. A merged span is
/// counted once, under the category of the region that claimed it first —
/// widest-first at the same start — so "38 secrets redacted" counts secrets, not
/// the number of rules that happened to agree about one.
pub(crate) fn apply(
    input: &str,
    mut regions: Vec<Region>,
    placeholder: &str,
) -> (String, Vec<Category>) {
    if regions.is_empty() {
        return (input.to_string(), Vec::new());
    }

    regions.sort_by(|a, b| {
        a.start
            .cmp(&b.start)
            // Widest first at a tie, so the merged span keeps the label of the
            // layer that saw the most context (a whole DSN beats a token inside it).
            .then(b.end.cmp(&a.end))
            .then(a.category.cmp(&b.category))
    });

    let mut merged: Vec<Region> = Vec::with_capacity(regions.len());
    for region in regions {
        match merged.last_mut() {
            Some(last) if region.start <= last.end => {
                last.end = last.end.max(region.end);
            }
            _ => merged.push(region),
        }
    }

    let mut out = String::with_capacity(input.len());
    let mut counts = Vec::with_capacity(merged.len());
    let mut cursor = 0usize;
    for region in merged {
        // A layer can only ever report offsets it found in `input`, but char
        // boundaries are worth asserting cheaply rather than panicking on a
        // slice: redaction must never crash the capture path.
        if region.start < cursor || region.end > input.len() {
            continue;
        }
        if !input.is_char_boundary(region.start) || !input.is_char_boundary(region.end) {
            continue;
        }
        out.push_str(&input[cursor..region.start]);
        out.push_str(placeholder);
        counts.push(region.category);
        cursor = region.end;
    }
    out.push_str(&input[cursor..]);
    (out, counts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_regions_merge_into_one_replacement() {
        let (out, counts) = apply(
            "abcdefghij",
            vec![
                Region::new(2, 6, Category::Entropy),
                Region::new(4, 8, Category::VendorRule),
            ],
            "[X]",
        );
        assert_eq!(out, "ab[X]ij");
        assert_eq!(counts, vec![Category::Entropy]);
    }

    #[test]
    fn adjacent_regions_merge_because_end_equals_start() {
        let (out, _) = apply(
            "abcdef",
            vec![
                Region::new(1, 3, Category::Entropy),
                Region::new(3, 5, Category::Entropy),
            ],
            "[X]",
        );
        assert_eq!(out, "a[X]f");
    }

    #[test]
    fn disjoint_regions_are_counted_separately() {
        let (out, counts) = apply(
            "aaabbbccc",
            vec![
                Region::new(0, 3, Category::Entropy),
                Region::new(6, 9, Category::CredentialValue),
            ],
            "[X]",
        );
        assert_eq!(out, "[X]bbb[X]");
        assert_eq!(counts, vec![Category::Entropy, Category::CredentialValue]);
    }

    #[test]
    fn a_region_landing_mid_character_is_dropped_rather_than_panicking() {
        // "é" is two bytes; offset 1 is not a char boundary.
        let (out, counts) = apply("é", vec![Region::new(0, 1, Category::Entropy)], "[X]");
        assert_eq!(out, "é");
        assert!(counts.is_empty());
    }
}
