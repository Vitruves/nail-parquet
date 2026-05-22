//! Fuzzy "did you mean?" suggestions for user-facing errors.
//!
//! Uses bounded Levenshtein distance with a tolerance that scales with input
//! length, so short typos (`pric` vs `price`) suggest, but unrelated tokens
//! don't (`apple` vs `price`).

/// Return the best fuzzy match for `input` among `candidates`, if any is close
/// enough to be worth suggesting. Case-insensitive.
pub fn suggest_close<'a, I>(input: &str, candidates: I) -> Option<String>
where
	I: IntoIterator<Item = &'a String>,
{
	let needle = input.to_lowercase();
	// Tolerance: at least 2 (catches one transposition) and at most 3,
	// scaling roughly with input length.
	let tolerance = (needle.chars().count() / 2).clamp(2, 3);

	let mut best: Option<(usize, &String)> = None;
	for cand in candidates {
		let d = levenshtein(&needle, &cand.to_lowercase());
		if d <= tolerance && best.map(|(bd, _)| d < bd).unwrap_or(true) {
			best = Some((d, cand));
		}
	}
	best.map(|(_, s)| s.clone())
}

/// Render a "Did you mean ..." suffix, or empty string when nothing close.
pub fn did_you_mean_suffix<'a, I>(input: &str, candidates: I) -> String
where
	I: IntoIterator<Item = &'a String>,
{
	match suggest_close(input, candidates) {
		Some(s) => format!(" Did you mean '{}'?", s),
		None => String::new(),
	}
}

fn levenshtein(a: &str, b: &str) -> usize {
	let a: Vec<char> = a.chars().collect();
	let b: Vec<char> = b.chars().collect();
	if a.is_empty() {
		return b.len();
	}
	if b.is_empty() {
		return a.len();
	}
	let mut prev: Vec<usize> = (0..=b.len()).collect();
	let mut curr: Vec<usize> = vec![0; b.len() + 1];
	for (i, ca) in a.iter().enumerate() {
		curr[0] = i + 1;
		for (j, cb) in b.iter().enumerate() {
			let cost = if ca == cb { 0 } else { 1 };
			curr[j + 1] = (curr[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost);
		}
		std::mem::swap(&mut prev, &mut curr);
	}
	prev[b.len()]
}

#[cfg(test)]
mod tests {
	use super::*;

	fn v(s: &[&str]) -> Vec<String> {
		s.iter().map(|x| x.to_string()).collect()
	}

	#[test]
	fn suggests_one_char_typo() {
		let cands = v(&["price", "quantity", "name"]);
		assert_eq!(
			suggest_close("pricr", cands.iter()).as_deref(),
			Some("price")
		);
	}

	#[test]
	fn suggests_case_insensitive() {
		let cands = v(&["Price", "Quantity"]);
		assert_eq!(
			suggest_close("price", cands.iter()).as_deref(),
			Some("Price")
		);
	}

	#[test]
	fn no_suggestion_when_too_different() {
		let cands = v(&["price", "quantity"]);
		assert_eq!(suggest_close("xyzab", cands.iter()), None);
	}

	#[test]
	fn suggests_transposition() {
		let cands = v(&["Email", "Name", "id"]);
		assert_eq!(
			suggest_close("Emial", cands.iter()).as_deref(),
			Some("Email")
		);
	}

	#[test]
	fn no_suggestion_when_empty_candidates() {
		assert_eq!(suggest_close("x", std::iter::empty::<&String>()), None);
	}

	#[test]
	fn picks_closest_when_multiple_in_range() {
		let cands = v(&["price", "pricer", "prize"]);
		// "pric" -> "price" (distance 1) beats "prize" (distance 2).
		assert_eq!(
			suggest_close("pric", cands.iter()).as_deref(),
			Some("price")
		);
	}
}
