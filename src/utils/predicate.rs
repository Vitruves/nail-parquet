//! Shared expression / condition engine used by `create`, `filter`, and `drop`.
//!
//! The goal is a single, newbie-friendly condition language that degrades to
//! nothing — anything you can write in SQL still works, and on top of that you
//! get math-style sugar so you never *have* to know SQL:
//!
//! * Comparisons: `=` `==` `!=` `<>` `<` `<=` `>` `>=`
//! * Chained ranges: `4 < x < 8`, `8 > x > 4`, `0 <= score <= 100`
//!   (expanded to `(4 < x) AND (x < 8)` …)
//! * Boolean glue: SQL `AND` / `OR` / `NOT`, **or** the shorthand `,` = AND and
//!   `|` = OR (AND binds tighter than OR, matching the old `filter` behavior)
//! * Full SQL set/range/null/pattern ops pass straight through: `BETWEEN a AND b`,
//!   `IN (…)`, `LIKE` / `ILIKE`, `IS NULL`, `IS NOT NULL`, `CASE WHEN …`
//! * Functions and aggregate sugar from `create` (`mean`, `pow`, `std`, …) work
//!   anywhere via [`rewrite_expression`]
//! * Column names are matched case-insensitively and quoted automatically, so
//!   `age > 18` works even when the column is `Age`
//!
//! [`normalize_predicate`] turns a friendly condition into a single SQL boolean
//! expression string; callers wrap it in `WHERE …` (or `WHERE NOT (…)` to drop).

use crate::error::{NailError, NailResult};

struct FunctionAlias {
	alias: &'static str,
	target: &'static str,
	is_aggregate: bool,
}

const FUNCTION_ALIASES: &[FunctionAlias] = &[
	FunctionAlias {
		alias: "mean",
		target: "avg",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "avg",
		target: "avg",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "sum",
		target: "sum",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "min",
		target: "min",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "max",
		target: "max",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "count",
		target: "count",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "median",
		target: "median",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "std",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stdev",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stddev",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stddev_samp",
		target: "stddev_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "stddev_pop",
		target: "stddev_pop",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "var",
		target: "var_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "variance",
		target: "var_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "var_samp",
		target: "var_samp",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "var_pop",
		target: "var_pop",
		is_aggregate: true,
	},
	FunctionAlias {
		alias: "pow",
		target: "power",
		is_aggregate: false,
	},
];

fn lookup_alias(name: &str) -> Option<&'static FunctionAlias> {
	let lower = name.to_ascii_lowercase();
	FUNCTION_ALIASES.iter().find(|f| f.alias == lower)
}

/// SQL keywords and literals that look like bare identifiers but must never be
/// treated as column references during identifier resolution.
const SQL_KEYWORDS: &[&str] = &[
	"and", "or", "not", "between", "in", "like", "ilike", "is", "null", "true", "false", "case",
	"when", "then", "else", "end", "as", "distinct", "interval", "cast", "asc", "desc", "escape",
];

fn is_sql_keyword(lower: &str) -> bool {
	SQL_KEYWORDS.contains(&lower)
}

/// Split a string on top-level occurrences of `sep`, ignoring separators inside
/// parentheses or quoted strings.
fn split_top_level(input: &str, sep: u8) -> Vec<String> {
	let bytes = input.as_bytes();
	let mut parts = Vec::new();
	let mut start = 0;
	let mut depth = 0i32;
	let mut in_single = false;
	let mut in_double = false;
	for (i, &c) in bytes.iter().enumerate() {
		if in_single {
			if c == b'\'' {
				in_single = false;
			}
			continue;
		}
		if in_double {
			if c == b'"' {
				in_double = false;
			}
			continue;
		}
		match c {
			b'\'' => in_single = true,
			b'"' => in_double = true,
			b'(' => depth += 1,
			b')' => depth -= 1,
			c if c == sep && depth == 0 => {
				parts.push(input[start..i].to_string());
				start = i + 1;
			}
			_ => {}
		}
	}
	parts.push(input[start..].to_string());
	parts
}

/// Split a column-spec string on top-level commas, ignoring commas inside
/// parentheses or quoted strings so calls like `pow(a, b)` and `round(x, 2)`
/// survive intact.
pub fn split_top_level_commas(input: &str) -> Vec<String> {
	split_top_level(input, b',')
}

fn find_matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
	debug_assert_eq!(bytes[open], b'(');
	let mut depth = 0i32;
	let mut i = open;
	let mut in_single = false;
	let mut in_double = false;
	while i < bytes.len() {
		let c = bytes[i];
		if in_single {
			if c == b'\'' {
				in_single = false;
			}
		} else if in_double {
			if c == b'"' {
				in_double = false;
			}
		} else {
			match c {
				b'\'' => in_single = true,
				b'"' => in_double = true,
				b'(' => depth += 1,
				b')' => {
					depth -= 1;
					if depth == 0 {
						return Some(i);
					}
				}
				_ => {}
			}
		}
		i += 1;
	}
	None
}

/// Rewrite a nail expression into a DataFusion SQL expression:
/// * Apply curated function aliases (mean → avg, std → stddev_samp, pow → power, ...).
/// * Wrap aggregate calls with `OVER ()` so they broadcast across rows (unless the
///   user already supplied an `OVER` clause).
/// * Leave unknown function calls, identifiers, quoted strings, and operators
///   untouched so they reach DataFusion as written.
pub fn rewrite_expression(expr: &str) -> String {
	let bytes = expr.as_bytes();
	let mut out = String::new();
	let mut i = 0;
	while i < bytes.len() {
		let c = bytes[i];
		if c == b'\'' {
			out.push('\'');
			i += 1;
			while i < bytes.len() {
				let b = bytes[i];
				out.push(b as char);
				i += 1;
				if b == b'\'' {
					if i < bytes.len() && bytes[i] == b'\'' {
						out.push('\'');
						i += 1;
					} else {
						break;
					}
				}
			}
			continue;
		}
		if c == b'"' {
			out.push('"');
			i += 1;
			while i < bytes.len() {
				let b = bytes[i];
				out.push(b as char);
				i += 1;
				if b == b'"' {
					break;
				}
			}
			continue;
		}
		if c.is_ascii_alphabetic() || c == b'_' {
			let start = i;
			while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
				i += 1;
			}
			let ident = &expr[start..i];
			let mut j = i;
			while j < bytes.len() && bytes[j].is_ascii_whitespace() {
				j += 1;
			}
			if j < bytes.len() && bytes[j] == b'(' {
				if let Some(close) = find_matching_paren(bytes, j) {
					let (target_name, is_aggregate) = match lookup_alias(ident) {
						Some(a) => (a.target, a.is_aggregate),
						None => (ident, false),
					};
					let inner = &expr[j + 1..close];
					let rewritten_inner = rewrite_expression(inner);
					out.push_str(target_name);
					out.push_str(&expr[i..j]);
					out.push('(');
					out.push_str(&rewritten_inner);
					out.push(')');
					if is_aggregate {
						let mut k = close + 1;
						while k < bytes.len() && bytes[k].is_ascii_whitespace() {
							k += 1;
						}
						let has_over =
							k + 4 <= bytes.len() && expr[k..k + 4].eq_ignore_ascii_case("OVER");
						if !has_over {
							out.push_str(" OVER ()");
						}
					}
					i = close + 1;
					continue;
				}
			}
			out.push_str(ident);
			continue;
		}
		out.push(c as char);
		i += 1;
	}
	out
}

/// True if `term` contains a top-level boolean/SQL keyword, which means it is a
/// real SQL fragment and must NOT be treated as one operand of a range chain.
fn contains_bool_keyword(term: &str) -> bool {
	let bytes = term.as_bytes();
	let mut i = 0;
	let mut in_single = false;
	let mut in_double = false;
	while i < bytes.len() {
		let c = bytes[i];
		if in_single {
			if c == b'\'' {
				in_single = false;
			}
			i += 1;
			continue;
		}
		if in_double {
			if c == b'"' {
				in_double = false;
			}
			i += 1;
			continue;
		}
		match c {
			b'\'' => in_single = true,
			b'"' => in_double = true,
			_ if c.is_ascii_alphabetic() || c == b'_' => {
				let start = i;
				while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
					i += 1;
				}
				let lower = term[start..i].to_ascii_lowercase();
				if matches!(
					lower.as_str(),
					"and" | "or" | "not" | "between" | "in" | "like" | "ilike" | "is"
				) {
					return true;
				}
				continue;
			}
			_ => {}
		}
		i += 1;
	}
	false
}

/// Expand a math-style range chain (`a < x < b`, `8 > x > 4`, `0 <= s <= 100`)
/// into ANDed pairwise comparisons. Returns `None` when the atom is not a pure
/// chain of `<` / `<=` / `>` / `>=` (e.g. it mixes in `=`, `!=`, `<>`, or a
/// boolean keyword), in which case it should be left for SQL to handle as-is.
fn try_expand_chain(atom: &str) -> Option<String> {
	let bytes = atom.as_bytes();
	let mut depth = 0i32;
	let mut in_single = false;
	let mut in_double = false;
	let mut operands: Vec<String> = Vec::new();
	let mut ops: Vec<&'static str> = Vec::new();
	let mut start = 0usize;
	let mut i = 0usize;
	while i < bytes.len() {
		let c = bytes[i];
		if in_single {
			if c == b'\'' {
				in_single = false;
			}
			i += 1;
			continue;
		}
		if in_double {
			if c == b'"' {
				in_double = false;
			}
			i += 1;
			continue;
		}
		match c {
			b'\'' => {
				in_single = true;
				i += 1;
				continue;
			}
			b'"' => {
				in_double = true;
				i += 1;
				continue;
			}
			b'(' => {
				depth += 1;
				i += 1;
				continue;
			}
			b')' => {
				depth -= 1;
				i += 1;
				continue;
			}
			_ => {}
		}
		if depth == 0 && (c == b'<' || c == b'>') {
			let next = bytes.get(i + 1).copied();
			let op: &'static str = match (c, next) {
				(b'<', Some(b'=')) => "<=",
				(b'>', Some(b'=')) => ">=",
				// `<>` (not-equal) is not a range operator → bail.
				(b'<', Some(b'>')) => return None,
				(b'<', _) => "<",
				_ => ">",
			};
			operands.push(atom[start..i].trim().to_string());
			ops.push(op);
			i += op.len();
			start = i;
			continue;
		}
		// Any other top-level comparison char means this is not a pure range chain.
		if depth == 0 && (c == b'=' || c == b'!') {
			return None;
		}
		i += 1;
	}
	operands.push(atom[start..].trim().to_string());

	if ops.len() < 2 {
		return None;
	}
	if operands.iter().any(|o| o.is_empty()) {
		return None;
	}
	if operands.iter().any(|o| contains_bool_keyword(o)) {
		return None;
	}

	let parts: Vec<String> = ops
		.iter()
		.enumerate()
		.map(|(k, op)| format!("({} {} {})", operands[k], op, operands[k + 1]))
		.collect();
	Some(parts.join(" AND "))
}

/// Replace top-level `==` with `=` (a common newbie habit), leaving anything
/// inside quotes untouched.
fn normalize_equals(atom: &str) -> String {
	let bytes = atom.as_bytes();
	let mut out = String::with_capacity(atom.len());
	let mut i = 0;
	let mut in_single = false;
	let mut in_double = false;
	while i < bytes.len() {
		let c = bytes[i];
		if in_single {
			out.push(c as char);
			if c == b'\'' {
				in_single = false;
			}
			i += 1;
			continue;
		}
		if in_double {
			out.push(c as char);
			if c == b'"' {
				in_double = false;
			}
			i += 1;
			continue;
		}
		match c {
			b'\'' => {
				in_single = true;
				out.push('\'');
			}
			b'"' => {
				in_double = true;
				out.push('"');
			}
			b'=' if bytes.get(i + 1) == Some(&b'=') => {
				out.push('=');
				i += 2;
				continue;
			}
			_ => out.push(c as char),
		}
		i += 1;
	}
	out
}

/// Quote bare identifiers that case-insensitively match a real column, so users
/// don't have to know that DataFusion lowercases unquoted names. Identifiers
/// that are function calls (followed by `(`), SQL keywords, or already quoted
/// are left alone.
///
/// When `quote_unknown_as_string` is set, a bare word that matches no column and
/// is not a keyword/function is treated as an unquoted string value (`B` →
/// `'B'`). This makes newbie conditions like `status=active` mean
/// `status = 'active'` instead of a column-to-column comparison. It is only
/// enabled for conditions, never for scalar `create` expressions (where an
/// unknown name is a typo that should surface as an error).
fn resolve_identifiers(sql: &str, columns: &[String], quote_unknown_as_string: bool) -> String {
	use std::collections::HashMap;
	let map: HashMap<String, &str> = columns
		.iter()
		.map(|c| (c.to_ascii_lowercase(), c.as_str()))
		.collect();

	let bytes = sql.as_bytes();
	let mut out = String::with_capacity(sql.len());
	let mut i = 0;
	// Last non-whitespace byte already emitted, used to tell whether a bare word
	// sits on the right of a comparison (value position) or the left (column).
	let mut last_significant: Option<u8> = None;
	while i < bytes.len() {
		let c = bytes[i];
		if c == b'\'' {
			out.push('\'');
			i += 1;
			while i < bytes.len() {
				let b = bytes[i];
				out.push(b as char);
				i += 1;
				if b == b'\'' {
					if bytes.get(i) == Some(&b'\'') {
						out.push('\'');
						i += 1;
					} else {
						break;
					}
				}
			}
			last_significant = Some(b'\'');
			continue;
		}
		if c == b'"' {
			out.push('"');
			i += 1;
			while i < bytes.len() {
				let b = bytes[i];
				out.push(b as char);
				i += 1;
				if b == b'"' {
					break;
				}
			}
			last_significant = Some(b'"');
			continue;
		}
		if c.is_ascii_alphabetic() || c == b'_' {
			let start = i;
			while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
				i += 1;
			}
			let ident = &sql[start..i];
			// Look past whitespace: a following `(` means this is a function call.
			let mut j = i;
			while j < bytes.len() && bytes[j].is_ascii_whitespace() {
				j += 1;
			}
			let is_call = bytes.get(j) == Some(&b'(');
			let lower = ident.to_ascii_lowercase();
			// A bare word in value position (right after a comparison operator)
			// that matches no column is read as an unquoted string value.
			let in_value_position =
				matches!(last_significant, Some(b'=') | Some(b'<') | Some(b'>'));
			if !is_call && !is_sql_keyword(&lower) {
				if let Some(actual) = map.get(&lower) {
					out.push('"');
					out.push_str(actual);
					out.push('"');
					last_significant = Some(b'"');
					continue;
				}
				if quote_unknown_as_string && in_value_position {
					out.push('\'');
					out.push_str(&ident.replace('\'', "''"));
					out.push('\'');
					last_significant = Some(b'\'');
					continue;
				}
			}
			out.push_str(ident);
			last_significant = ident.as_bytes().last().copied();
			continue;
		}
		out.push(c as char);
		if !c.is_ascii_whitespace() {
			last_significant = Some(c);
		}
		i += 1;
	}
	out
}

/// Normalize a scalar value expression (e.g. a `create` column body like
/// `price*quantity`): apply function aliases/aggregate windowing and resolve
/// bare column references case-insensitively. Unlike [`normalize_predicate`]
/// this does no boolean splitting or range-chain expansion.
pub fn normalize_expression(expr: &str, columns: &[String]) -> String {
	resolve_identifiers(&rewrite_expression(expr), columns, false)
}

/// Normalize a single AND-term (no top-level `,` or `|`) into SQL.
fn normalize_atom(atom: &str) -> NailResult<String> {
	let atom = atom.trim();
	if atom.is_empty() {
		return Err(NailError::InvalidArgument(
			"Empty condition between separators".to_string(),
		));
	}
	let expanded = match try_expand_chain(atom) {
		Some(chained) => chained,
		None => normalize_equals(atom),
	};
	Ok(rewrite_expression(&expanded))
}

/// Turn a friendly condition string into a single SQL boolean expression.
///
/// `columns` are the data frame's field names, used to case-insensitively
/// resolve and quote bare column references. Precedence: `,`/AND bind tighter
/// than `|`/OR, so `a=1,b=2|c=3` becomes `(a=1 AND b=2) OR c=3`.
pub fn normalize_predicate(input: &str, columns: &[String]) -> NailResult<String> {
	let trimmed = input.trim();
	if trimmed.is_empty() {
		return Err(NailError::InvalidArgument(
			"Empty condition expression".to_string(),
		));
	}

	let mut or_parts: Vec<String> = Vec::new();
	for or_group in split_top_level(trimmed, b'|') {
		let group = or_group.trim();
		if group.is_empty() {
			return Err(NailError::InvalidArgument(format!(
				"Empty OR group in condition: {}",
				input
			)));
		}
		let mut and_parts: Vec<String> = Vec::new();
		for term in split_top_level(group, b',') {
			and_parts.push(normalize_atom(&term)?);
		}
		or_parts.push(and_parts.join(" AND "));
	}

	let combined = if or_parts.len() == 1 {
		or_parts.pop().unwrap()
	} else {
		or_parts
			.into_iter()
			.map(|p| format!("({})", p))
			.collect::<Vec<_>>()
			.join(" OR ")
	};

	Ok(resolve_identifiers(&combined, columns, true))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn cols() -> Vec<String> {
		vec!["Age".into(), "Name".into(), "status".into(), "score".into()]
	}

	fn norm(s: &str) -> String {
		normalize_predicate(s, &cols()).unwrap()
	}

	#[test]
	fn simple_comparison_resolves_case() {
		assert_eq!(norm("age > 18"), "\"Age\" > 18");
		assert_eq!(norm("NAME = 'Bob'"), "\"Name\" = 'Bob'");
	}

	#[test]
	fn double_equals_becomes_single() {
		assert_eq!(norm("age == 18"), "\"Age\" = 18");
	}

	#[test]
	fn comma_is_and_pipe_is_or() {
		assert_eq!(
			norm("age>=18,status=active"),
			"\"Age\">=18 AND \"status\"='active'"
		);
		assert_eq!(
			norm("status=a|status=b"),
			"(\"status\"='a') OR (\"status\"='b')"
		);
	}

	#[test]
	fn and_binds_tighter_than_or() {
		assert_eq!(
			norm("age>=18,score<50|status=admin"),
			"(\"Age\">=18 AND \"score\"<50) OR (\"status\"='admin')"
		);
	}

	#[test]
	fn range_chain_expands() {
		assert_eq!(norm("4 < age < 8"), "(4 < \"Age\") AND (\"Age\" < 8)");
	}

	#[test]
	fn range_chain_greater_first() {
		assert_eq!(norm("8 > age > 4"), "(8 > \"Age\") AND (\"Age\" > 4)");
		assert_eq!(
			norm("0 <= score <= 100"),
			"(0 <= \"score\") AND (\"score\" <= 100)"
		);
	}

	#[test]
	fn sql_operators_pass_through() {
		assert_eq!(norm("age BETWEEN 18 AND 65"), "\"Age\" BETWEEN 18 AND 65");
		assert_eq!(norm("status IN ('a','b')"), "\"status\" IN ('a','b')");
		assert_eq!(norm("name LIKE 'A%'"), "\"Name\" LIKE 'A%'");
		assert_eq!(norm("score IS NULL"), "\"score\" IS NULL");
	}

	#[test]
	fn not_a_chain_when_keyword_present() {
		// `age > 4 AND age < 8` is valid SQL; must not be mangled by chain logic.
		assert_eq!(norm("age > 4 AND age < 8"), "\"Age\" > 4 AND \"Age\" < 8");
	}

	#[test]
	fn quoted_identifier_left_alone() {
		// already-quoted names are respected verbatim
		assert_eq!(norm("\"Age\" > 18"), "\"Age\" > 18");
	}

	#[test]
	fn function_alias_and_call_not_resolved_as_column() {
		// `count` is a function here (followed by paren), not the absent column.
		assert_eq!(norm("abs(score) > 2"), "abs(\"score\") > 2");
	}

	#[test]
	fn unquoted_value_becomes_string_literal() {
		// Newbie-friendly: bare RHS word is a value, not a column reference.
		assert_eq!(norm("status=active"), "\"status\"='active'");
		assert_eq!(norm("name != Bob"), "\"Name\" != 'Bob'");
	}

	#[test]
	fn column_vs_column_when_both_match() {
		// Both sides are real columns → compared as columns, not stringified.
		assert_eq!(norm("score > age"), "\"score\" > \"Age\"");
	}

	#[test]
	fn empty_is_error() {
		assert!(normalize_predicate("", &cols()).is_err());
		assert!(normalize_predicate("age>1,,b=2", &cols()).is_err());
	}
}
