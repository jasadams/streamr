//! Compiled ANSI SQL/JSON path representation.
//!
//! One implementation is shared by the JSON_VALUE / JSON_QUERY / JSON_EXISTS
//! extraction kernels, JSON_OBJECT construction validation, and (later) the
//! JSON_TABLE row-expansion ticket. Literal paths are compiled once — at plan
//! time for validation diagnostics and memoized per process for execution —
//! never reparsed per row.
//!
//! The accepted dialect is deliberately narrower than full JSONPath. Only the
//! catalog subset is accepted; anything else is a precise diagnostic naming
//! the path and the offending segment. Explicit `LAX`/`STRICT` prefixes are
//! rejected rather than approximated. Evaluation uses standard lax semantics:
//! a missing member yields an empty sequence, while a present JSON null is a
//! real path item.

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::common::{DataFusionError, Result};
use serde_json::Value;

/// Maximum number of path steps (nesting depth of the path itself).
pub const MAX_PATH_DEPTH: usize = 64;
/// Maximum path source length in characters.
pub const MAX_PATH_LENGTH: usize = 4096;
/// Maximum number of items any single evaluation step may produce.
pub const MAX_PATH_ITEMS: usize = 1024;
/// Maximum JSON document size accepted by the extraction kernels.
pub const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
/// Maximum serialized output size produced by JSON_QUERY / JSON_OBJECT.
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

/// A path syntax or bound violation, reported as a planner diagnostic when a
/// literal path is compiled and as a hard execution error otherwise. Messages
/// always carry the complete path source and the offending segment; nothing
/// is truncated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathError {
    path: String,
    offset: usize,
    message: String,
}

impl PathError {
    fn new(path: &str, offset: usize, message: impl Into<String>) -> Self {
        Self {
            path: path.to_string(),
            offset,
            message: message.into(),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for PathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid SQL/JSON path '{}': {} (at offset {})",
            self.path, self.message, self.offset
        )
    }
}

impl std::error::Error for PathError {}

/// One evaluation failure that must surface as an engine error rather than a
/// data-conversion `NULL`/`FALSE` (for example, an expansion bound).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalError {
    path: String,
    message: String,
}

impl EvalError {
    fn new(path: &str, message: impl Into<String>) -> Self {
        Self {
            path: path.to_string(),
            message: message.into(),
        }
    }
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SQL/JSON path '{}' evaluation failed: {}",
            self.path, self.message
        )
    }
}

impl std::error::Error for EvalError {}

impl From<EvalError> for DataFusionError {
    fn from(value: EvalError) -> Self {
        DataFusionError::ResourcesExhausted(value.to_string())
    }
}

/// A scalar literal allowed inside a filter predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterLiteral {
    Str(String),
    Number(serde_json::Number),
    Bool(bool),
}

/// A filter predicate over `@` or `@.member...`.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterPredicate {
    /// Member chain applied to `@`; empty means `@` itself.
    subject: Vec<String>,
    op: FilterOp,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterOp {
    /// `== null`: true only for a present JSON null, never for absence.
    EqualsNull,
    EqualsLiteral(FilterLiteral),
}

/// One compiled path step.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// `.name` member traversal (lax: missing member contributes nothing;
    /// member access over an array projects element-wise).
    Member(String),
    /// `[*]` array wildcard (lax: a non-array item is passed through).
    Wildcard,
    /// `? (predicate)`.
    Filter(FilterPredicate),
    /// `.keyvalue()` — expands each object item into key/value/id items.
    KeyValue,
}

/// One item produced by path evaluation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PathItem<'a> {
    /// A borrowed document value, including JSON null.
    Value(&'a Value),
    /// A borrowed string (the `key` field of a keyvalue item).
    Str(&'a str),
    /// The `id` field of a keyvalue item.
    Id(u64),
    /// A synthetic `{key, value, id}` item produced by `.keyvalue()`.
    KeyValue {
        key: &'a str,
        value: &'a Value,
        id: u64,
    },
}

impl PathItem<'_> {
    /// True for every produced item, including JSON null: an item's presence
    /// is what JSON_EXISTS reports.
    pub fn is_present(&self) -> bool {
        true
    }
}

/// A validated, compiled SQL/JSON path.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledPath {
    source: String,
    steps: Vec<Step>,
}

impl CompiledPath {
    /// Parse and validate a path literal, enforcing the dialect subset and
    /// the depth/length bounds.
    pub fn compile(path: &str) -> std::result::Result<Self, PathError> {
        if path.chars().count() > MAX_PATH_LENGTH {
            return Err(PathError::new(
                path,
                MAX_PATH_LENGTH,
                format!("path exceeds maximum length of {MAX_PATH_LENGTH} characters"),
            ));
        }
        let trimmed = path.trim();
        let lower = trimmed.to_ascii_lowercase();
        let mode_prefix = if lower.starts_with("lax") {
            Some(&trimmed[3..])
        } else if lower.starts_with("strict") {
            Some(&trimmed[6..])
        } else {
            None
        };
        if mode_prefix
            .is_some_and(|rest| rest.starts_with(char::is_whitespace) || rest.starts_with('$'))
        {
            return Err(PathError::new(
                path,
                0,
                "explicit LAX/STRICT path mode is not supported; paths are always evaluated in lax mode",
            ));
        }
        let mut parser = PathParser {
            source: trimmed,
            bytes: trimmed.as_bytes(),
            pos: 0,
        };
        let steps = parser.parse_steps()?;
        Ok(Self {
            source: path.to_string(),
            steps,
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// Evaluate the path against a parsed document using lax semantics.
    ///
    /// Returns every matching item in document order. Expansion is bounded by
    /// [`MAX_PATH_ITEMS`]; exceeding it is an [`EvalError`] (an engine
    /// failure), never a silent empty result.
    pub fn evaluate<'a>(
        &self,
        doc: &'a Value,
    ) -> std::result::Result<Vec<PathItem<'a>>, EvalError> {
        let mut items = vec![PathItem::Value(doc)];
        for (index, step) in self.steps.iter().enumerate() {
            let mut next: Vec<PathItem<'a>> = Vec::new();
            for item in &items {
                match step {
                    Step::Member(name) => member_step(item, name, &mut next),
                    Step::Wildcard => wildcard_step(item, &mut next),
                    Step::KeyValue => keyvalue_step(item, &mut next),
                    Step::Filter(predicate) => filter_step(item, predicate, &mut next)?,
                }
                if next.len() > MAX_PATH_ITEMS {
                    return Err(EvalError::new(
                        &self.source,
                        format!(
                            "step {} ('{}') expanded beyond the maximum of {MAX_PATH_ITEMS} items",
                            index + 1,
                            step_display(step),
                        ),
                    ));
                }
            }
            items = next;
        }
        Ok(items)
    }
}

fn step_display(step: &Step) -> String {
    match step {
        Step::Member(name) => format!(".{name}"),
        Step::Wildcard => "[*]".to_string(),
        Step::Filter(predicate) => {
            let subject = if predicate.subject.is_empty() {
                "@".to_string()
            } else {
                format!("@.{}", predicate.subject.join("."))
            };
            let rhs = match &predicate.op {
                FilterOp::EqualsNull => "null".to_string(),
                FilterOp::EqualsLiteral(literal) => literal_display(literal),
            };
            format!("? ({subject} == {rhs})")
        }
        Step::KeyValue => ".keyvalue()".to_string(),
    }
}

fn literal_display(literal: &FilterLiteral) -> String {
    match literal {
        FilterLiteral::Str(s) => format!("\"{s}\""),
        FilterLiteral::Number(n) => n.to_string(),
        FilterLiteral::Bool(b) => b.to_string(),
    }
}

fn member_step<'a>(item: &PathItem<'a>, name: &str, out: &mut Vec<PathItem<'a>>) {
    match item {
        PathItem::Value(Value::Object(map)) => {
            if let Some(value) = map.get(name) {
                out.push(PathItem::Value(value));
            }
        }
        // Standard lax auto-adaptation: member access over an array projects
        // over its elements.
        PathItem::Value(Value::Array(elements)) => {
            for element in elements {
                if let Value::Object(map) = element
                    && let Some(value) = map.get(name)
                {
                    out.push(PathItem::Value(value));
                }
            }
        }
        // Synthetic keyvalue items expose `key`, `value` and `id` members.
        PathItem::KeyValue { key, value, id } => match name {
            "key" => out.push(PathItem::Str(key)),
            "value" => out.push(PathItem::Value(value)),
            "id" => out.push(PathItem::Id(*id)),
            _ => {}
        },
        _ => {}
    }
}

fn wildcard_step<'a>(item: &PathItem<'a>, out: &mut Vec<PathItem<'a>>) {
    match item {
        PathItem::Value(Value::Array(elements)) => {
            for element in elements {
                out.push(PathItem::Value(element));
            }
        }
        // Lax adaptation: a non-array item passes through unchanged.
        other => out.push(*other),
    }
}

fn keyvalue_step<'a>(item: &PathItem<'a>, out: &mut Vec<PathItem<'a>>) {
    if let PathItem::Value(Value::Object(map)) = item {
        for (id, (key, value)) in map.iter().enumerate() {
            out.push(PathItem::KeyValue {
                key: key.as_str(),
                value,
                id: id as u64,
            });
        }
    }
}

/// Lax filter auto-adaptation: a filter over an array applies its predicate
/// to each element; any other item is tested directly. Matching items are
/// kept in order.
fn filter_step<'a>(
    item: &PathItem<'a>,
    predicate: &FilterPredicate,
    out: &mut Vec<PathItem<'a>>,
) -> std::result::Result<(), EvalError> {
    match item {
        PathItem::Value(Value::Array(elements)) => {
            for element in elements {
                let element_item = PathItem::Value(element);
                if predicate.matches(&element_item)? {
                    out.push(element_item);
                }
            }
        }
        other => {
            if predicate.matches(other)? {
                out.push(*other);
            }
        }
    }
    Ok(())
}

/// Resolve `@` or `@.member...` from one item to a comparison target.
enum Target<'a> {
    Value(&'a Value),
    Str(&'a str),
    Id(u64),
    /// The subject chain did not resolve (lax absence). Absence is never
    /// equal to a present JSON null.
    Missing,
}

fn resolve_subject<'a>(item: &PathItem<'a>, subject: &[String]) -> Target<'a> {
    if subject.is_empty() {
        return match item {
            PathItem::Value(value) => Target::Value(value),
            PathItem::Str(s) => Target::Str(s),
            PathItem::Id(id) => Target::Id(*id),
            // `@` on a keyvalue item addresses the synthetic object; compare
            // `.key`/`.value`/`.id` explicitly instead.
            PathItem::KeyValue { .. } => Target::Missing,
        };
    }
    // Synthetic keyvalue items expose `key`, `value` and `id` members.
    if let PathItem::KeyValue { key, value, id } = item {
        return match subject[0].as_str() {
            "key" if subject.len() == 1 => Target::Str(key),
            "value" => resolve_member_chain(Target::Value(value), &subject[1..]),
            "id" if subject.len() == 1 => Target::Id(*id),
            _ => Target::Missing,
        };
    }
    // Walk the member chain from the item.
    let start = match item {
        PathItem::Value(value) => Target::Value(value),
        PathItem::Str(_) | PathItem::Id(_) | PathItem::KeyValue { .. } => Target::Missing,
    };
    resolve_member_chain(start, subject)
}

fn resolve_member_chain<'a>(start: Target<'a>, names: &[String]) -> Target<'a> {
    let mut current = start;
    for name in names {
        let map = match current {
            Target::Value(Value::Object(map)) => map,
            _ => return Target::Missing,
        };
        current = match map.get(name) {
            Some(value) => Target::Value(value),
            None => return Target::Missing,
        };
    }
    current
}

fn target_equals_null(target: &Target<'_>) -> bool {
    matches!(target, Target::Value(Value::Null))
}

fn target_equals_literal(target: &Target<'_>, literal: &FilterLiteral) -> bool {
    match (target, literal) {
        (Target::Value(Value::String(s)), FilterLiteral::Str(expected)) => s == expected,
        (Target::Str(s), FilterLiteral::Str(expected)) => s == expected,
        // serde_json's Number equality is exact: JSON `1` (integer) does not
        // equal the literal `1.0` (float), and vice versa. The catalog only
        // requires string and null filters, so this numeric strictness is a
        // deliberate superset beyond the contract, not an approximation of
        // SQL numeric comparison.
        (Target::Value(Value::Number(n)), FilterLiteral::Number(expected)) => n == expected,
        (Target::Id(id), FilterLiteral::Number(expected)) => {
            serde_json::Number::from(*id) == *expected
        }
        (Target::Value(Value::Bool(b)), FilterLiteral::Bool(expected)) => b == expected,
        _ => false,
    }
}

impl FilterPredicate {
    fn matches(&self, item: &PathItem<'_>) -> std::result::Result<bool, EvalError> {
        let target = resolve_subject(item, &self.subject);
        Ok(match &self.op {
            FilterOp::EqualsNull => target_equals_null(&target),
            FilterOp::EqualsLiteral(literal) => target_equals_literal(&target, literal),
        })
    }
}

struct PathParser<'a> {
    source: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> PathParser<'a> {
    fn error<T>(
        &self,
        offset: usize,
        message: impl Into<String>,
    ) -> std::result::Result<T, PathError> {
        Err(PathError::new(self.source, offset, message))
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn parse_steps(&mut self) -> std::result::Result<Vec<Step>, PathError> {
        self.skip_ws();
        if self.peek() != Some(b'$') {
            return self.error(self.pos, "path must begin with the root marker '$'");
        }
        self.pos += 1;
        let mut steps = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                None => break,
                Some(b'.') => {
                    if steps.len() >= MAX_PATH_DEPTH {
                        return self.error(
                            self.pos,
                            format!("path exceeds maximum depth of {MAX_PATH_DEPTH} steps"),
                        );
                    }
                    self.pos += 1;
                    let name = self.parse_member_name()?;
                    if name == "keyvalue" && self.peek() == Some(b'(') {
                        self.pos += 1;
                        self.skip_ws();
                        if self.peek() != Some(b')') {
                            return self.error(self.pos, "expected ')' to close .keyvalue()");
                        }
                        self.pos += 1;
                        steps.push(Step::KeyValue);
                    } else {
                        steps.push(Step::Member(name));
                    }
                }
                Some(b'[') => {
                    if steps.len() >= MAX_PATH_DEPTH {
                        return self.error(
                            self.pos,
                            format!("path exceeds maximum depth of {MAX_PATH_DEPTH} steps"),
                        );
                    }
                    self.pos += 1;
                    self.skip_ws();
                    if self.peek() != Some(b'*') {
                        return self.error(
                            self.pos,
                            "unsupported path segment; only the array wildcard '[*]' is accepted inside '[' ']'",
                        );
                    }
                    self.pos += 1;
                    self.skip_ws();
                    if self.peek() != Some(b']') {
                        return self.error(self.pos, "expected ']' after the array wildcard");
                    }
                    self.pos += 1;
                    steps.push(Step::Wildcard);
                }
                Some(b'?') => {
                    if steps.len() >= MAX_PATH_DEPTH {
                        return self.error(
                            self.pos,
                            format!("path exceeds maximum depth of {MAX_PATH_DEPTH} steps"),
                        );
                    }
                    self.pos += 1;
                    self.skip_ws();
                    if self.peek() != Some(b'(') {
                        return self.error(self.pos, "expected '(' after '?' in a filter step");
                    }
                    self.pos += 1;
                    steps.push(Step::Filter(self.parse_predicate()?));
                    self.skip_ws();
                    if self.peek() != Some(b')') {
                        return self.error(self.pos, "expected ')' to close the filter predicate");
                    }
                    self.pos += 1;
                }
                Some(_) => {
                    return self.error(
                        self.pos,
                        format!(
                            "unsupported path construct at '{}'; accepted steps are '.name', '[*]', '? (predicate)' and '.keyvalue()'",
                            &self.source[self.pos..]
                        ),
                    );
                }
            }
        }
        Ok(steps)
    }

    fn parse_member_name(&mut self) -> std::result::Result<String, PathError> {
        let start = self.pos;
        match self.peek() {
            Some(b) if b.is_ascii_alphabetic() || b == b'_' => self.pos += 1,
            _ => {
                return self.error(
                    self.pos,
                    "expected a member name after '.' (letters, digits and '_' only)",
                );
            }
        }
        while matches!(self.peek(), Some(b) if b.is_ascii_alphanumeric() || b == b'_') {
            self.pos += 1;
        }
        Ok(self.source[start..self.pos].to_string())
    }

    fn parse_predicate(&mut self) -> std::result::Result<FilterPredicate, PathError> {
        self.skip_ws();
        if self.peek() != Some(b'@') {
            return self.error(
                self.pos,
                "filter predicates must operate on '@' (the current item)",
            );
        }
        self.pos += 1;
        let mut subject = Vec::new();
        while self.peek() == Some(b'.') {
            self.pos += 1;
            subject.push(self.parse_member_name()?);
        }
        self.skip_ws();
        if self.peek() != Some(b'=') || self.bytes.get(self.pos + 1).copied() != Some(b'=') {
            return self.error(
                self.pos,
                "unsupported filter operator; only '==' comparisons are accepted",
            );
        }
        self.pos += 2;
        self.skip_ws();
        let op = match self.peek() {
            Some(b'"') => {
                let start = self.pos + 1;
                let mut end = start;
                while end < self.bytes.len() && self.bytes[end] != b'"' {
                    if self.bytes[end] == b'\\' {
                        return self.error(
                            end,
                            "escape sequences inside filter string literals are not supported",
                        );
                    }
                    end += 1;
                }
                if end >= self.bytes.len() {
                    return self.error(start, "unterminated string literal in filter predicate");
                }
                self.pos = end + 1;
                FilterOp::EqualsLiteral(FilterLiteral::Str(self.source[start..end].to_string()))
            }
            Some(b't') | Some(b'f') | Some(b'n') => {
                let rest = &self.source[self.pos..];
                if rest.starts_with("true") {
                    self.pos += 4;
                    FilterOp::EqualsLiteral(FilterLiteral::Bool(true))
                } else if rest.starts_with("false") {
                    self.pos += 5;
                    FilterOp::EqualsLiteral(FilterLiteral::Bool(false))
                } else if rest.starts_with("null") {
                    self.pos += 4;
                    FilterOp::EqualsNull
                } else {
                    return self.error(self.pos, "expected a literal after '=='");
                }
            }
            Some(b'-' | b'0'..=b'9') => {
                let start = self.pos;
                if self.peek() == Some(b'-') {
                    self.pos += 1;
                }
                let mut consumed_digits = false;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                    consumed_digits = true;
                }
                if self.peek() == Some(b'.') {
                    self.pos += 1;
                    while matches!(self.peek(), Some(b'0'..=b'9')) {
                        self.pos += 1;
                        consumed_digits = true;
                    }
                }
                if matches!(self.peek(), Some(b'e' | b'E')) {
                    self.pos += 1;
                    if matches!(self.peek(), Some(b'+' | b'-')) {
                        self.pos += 1;
                    }
                    let mut exponent_digits = false;
                    while matches!(self.peek(), Some(b'0'..=b'9')) {
                        self.pos += 1;
                        exponent_digits = true;
                    }
                    if !exponent_digits {
                        return self.error(self.pos, "expected digits in the numeric exponent");
                    }
                }
                if !consumed_digits {
                    return self.error(start, "expected a numeric literal after '=='");
                }
                let text = &self.source[start..self.pos];
                let number: serde_json::Number = match text.parse() {
                    Ok(number) => number,
                    Err(_) => {
                        return self.error(start, format!("invalid numeric literal '{text}'"));
                    }
                };
                FilterOp::EqualsLiteral(FilterLiteral::Number(number))
            }
            _ => {
                return self.error(
                    self.pos,
                    "expected a literal (double-quoted string, number, true, false or null) after '=='",
                );
            }
        };
        Ok(FilterPredicate { subject, op })
    }
}

/// Process-wide memo for compiled literal paths.
///
/// Literal paths are validated at plan time; execution looks the compiled
/// form up here so a path is parsed at most once per process rather than per
/// row. The cache is bounded; a full cache is cleared rather than growing
/// without limit.
#[derive(Default)]
pub struct PathCache {
    paths: Mutex<std::collections::HashMap<String, Arc<CompiledPath>>>,
}

impl PathCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            paths: Mutex::new(std::collections::HashMap::with_capacity(capacity)),
        }
    }

    /// Compile (or fetch a previously compiled) path. Syntax errors are
    /// returned as planning diagnostics so an invalid literal path fails at
    /// plan time, never as a runtime "missing field".
    pub fn compile(&self, path: &str) -> Result<Arc<CompiledPath>> {
        let mut paths = self.paths.lock().unwrap();
        if let Some(compiled) = paths.get(path) {
            return Ok(Arc::clone(compiled));
        }
        let compiled = Arc::new(
            CompiledPath::compile(path)
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
        );
        if paths.len() >= 4096 {
            paths.clear();
        }
        paths.insert(path.to_string(), Arc::clone(&compiled));
        Ok(compiled)
    }
}

/// A shared cache instance used by every SQL/JSON kernel.
pub fn shared_path_cache() -> &'static PathCache {
    static CACHE: std::sync::OnceLock<PathCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(PathCache::default)
}

/// Compile a literal path at plan time, producing a planner diagnostic that
/// names the full path and the offending segment on failure.
pub fn compile_literal_path(path: &str) -> Result<Arc<CompiledPath>> {
    shared_path_cache().compile(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn eval(path: &str, doc: Value) -> Vec<String> {
        let compiled = CompiledPath::compile(path).unwrap();
        compiled
            .evaluate(&doc)
            .unwrap()
            .iter()
            .map(|item| match item {
                PathItem::Value(value) => value.to_string(),
                PathItem::Str(s) => s.to_string(),
                PathItem::Id(id) => id.to_string(),
                PathItem::KeyValue { key, value, id } => {
                    format!("kv(key={key}, value={value}, id={id})")
                }
            })
            .collect()
    }

    #[test]
    fn root_yields_the_document() {
        assert_eq!(eval("$", json!({"a": 1})), vec!["{\"a\":1}"]);
    }

    #[test]
    fn member_traversal_and_lax_absence() {
        let doc = json!({"traits": {"name": "Jason"}});
        assert_eq!(eval("$.traits.name", doc.clone()), vec!["\"Jason\""]);
        assert!(eval("$.traits.missing", doc.clone()).is_empty());
        assert!(eval("$.absent.name", doc).is_empty());
    }

    #[test]
    fn json_null_is_a_real_item_not_absence() {
        let doc = json!({"traits": {"name": null}});
        assert_eq!(eval("$.traits.name", doc), vec!["null"]);
    }

    #[test]
    fn array_wildcard_and_lax_passthrough() {
        let doc = json!({"xs": [1, 2], "single": 3});
        assert_eq!(eval("$.xs[*]", doc.clone()), vec!["1", "2"]);
        assert_eq!(eval("$.single[*]", doc), vec!["3"]);
    }

    #[test]
    fn lax_member_access_projects_over_arrays() {
        let doc = json!({"ids": [{"v": 1}, {"v": 2}, {"other": 3}]});
        assert_eq!(eval("$.ids.v", doc), vec!["1", "2"]);
    }

    #[test]
    fn email_filter_over_wildcard() {
        let doc = json!({"identifiers": [
            {"identity_type": "email", "value": "a@example.test"},
            {"identity_type": "discord", "value": "d1"}
        ]});
        assert_eq!(
            eval(
                "$.identifiers[*] ? (@.identity_type == \"email\").value",
                doc
            ),
            vec!["\"a@example.test\""]
        );
    }

    #[test]
    fn discord_equivalent_filter() {
        let doc = json!({"identifiers": [
            {"identity_type": "email", "value": "a@example.test"},
            {"identity_type": "discord", "value": "d1"}
        ]});
        assert_eq!(
            eval(
                "$.identifiers[*] ? (@.identity_type == \"discord\").value",
                doc
            ),
            vec!["\"d1\""]
        );
    }

    #[test]
    fn explicit_null_test_matches_only_present_null() {
        let present_null = json!({"traits": {"name": null}});
        let missing = json!({"traits": {}});
        assert_eq!(
            eval("$.traits.name ? (@ == null)", present_null),
            vec!["null"]
        );
        assert!(eval("$.traits.name ? (@ == null)", missing).is_empty());
    }

    #[test]
    fn filter_directly_after_member() {
        let doc = json!({"identifiers": [
            {"identity_type": "email", "value": "a@example.test"},
            {"identity_type": "discord", "value": "d1"}
        ]});
        assert_eq!(
            eval("$.identifiers ? (@.identity_type == \"email\").value", doc),
            vec!["\"a@example.test\""]
        );
    }

    #[test]
    fn keyvalue_items_expose_key_value_id() {
        let doc = json!({"m": {"b": 2, "a": 1}});
        // serde_json's default map is ordered by key.
        assert_eq!(
            eval("$.m.keyvalue()", doc.clone()),
            vec!["kv(key=a, value=1, id=0)", "kv(key=b, value=2, id=1)"]
        );
        assert_eq!(eval("$.m.keyvalue().value", doc.clone()), vec!["1", "2"]);
        // `.key` materializes the bare key as a string item.
        assert_eq!(eval("$.m.keyvalue().key", doc), vec!["a", "b"]);
    }

    #[test]
    fn numeric_and_boolean_filter_literals() {
        let doc = json!({"xs": [{"n": 1, "b": true}, {"n": 2, "b": false}]});
        // serde_json's default map serializes keys in sorted order.
        assert_eq!(
            eval("$.xs[*] ? (@.n == 2)", doc.clone()),
            vec!["{\"b\":false,\"n\":2}"]
        );
        assert_eq!(
            eval("$.xs[*] ? (@.b == true)", doc),
            vec!["{\"b\":true,\"n\":1}"]
        );
    }

    #[test]
    fn invalid_paths_are_rejected_with_the_full_path_and_segment() {
        for (path, expected) in [
            ("$.a[0]", "only the array wildcard"),
            ("$.a[", "only the array wildcard"),
            ("a.b", "must begin with the root marker"),
            ("$.a ? (@.b != \"x\")", "only '==' comparisons"),
            ("$.a ? (1 == 1)", "must operate on '@'"),
            ("$.a ? (@.b == 'x')", "expected a literal"),
            ("$.a ? (@.b == \"x)", "unterminated string literal"),
            ("$.a ? (@.b == \"x\\\"y\")", "escape sequences"),
            ("$.a?.", "expected '(' after '?'"),
            ("LAX $.a", "explicit LAX/STRICT"),
            ("STRICT $.a", "explicit LAX/STRICT"),
            ("$.a..b", "expected a member name"),
            ("$.", "expected a member name"),
        ] {
            let error = CompiledPath::compile(path).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "path {path}: expected {expected:?}, got {error}"
            );
            assert!(
                error.to_string().contains(path),
                "error must quote the full path: {error}"
            );
        }
        // A member literally named `keyvalue` without parens is a normal
        // member step, not a syntax error.
        assert!(CompiledPath::compile("$.keyvalue2").is_ok());
    }

    #[test]
    fn keyvalue_requires_empty_parens() {
        assert!(CompiledPath::compile("$.a.keyvalue").is_ok());
        assert!(CompiledPath::compile("$.a.keyvalue()").is_ok());
        assert!(CompiledPath::compile("$.a.keyvalue(x)").is_err());
    }

    #[test]
    fn depth_and_length_bounds() {
        let deep = "$".to_string() + &".a".repeat(MAX_PATH_DEPTH + 1);
        let error = CompiledPath::compile(&deep).unwrap_err();
        assert!(error.to_string().contains("maximum depth"), "{error}");

        let long = "$".to_string() + &"a".repeat(MAX_PATH_LENGTH);
        let error = CompiledPath::compile(&long).unwrap_err();
        assert!(error.to_string().contains("maximum length"), "{error}");
    }

    #[test]
    fn expansion_bound_is_an_evaluation_error() {
        let doc = json!({"xs": (0..(MAX_PATH_ITEMS + 8)).collect::<Vec<_>>()});
        let compiled = CompiledPath::compile("$.xs[*]").unwrap();
        let error = compiled.evaluate(&doc).unwrap_err();
        assert!(error.to_string().contains("maximum"), "{error}");
    }

    #[test]
    fn whitespace_between_steps_is_accepted() {
        let doc = json!({"identifiers": [{"identity_type": "email", "value": "v"}]});
        assert_eq!(
            eval(
                "$.identifiers [*] ? (@.identity_type == \"email\") .value",
                doc
            ),
            vec!["\"v\""]
        );
    }

    #[test]
    fn cache_returns_the_same_compiled_instance() {
        let cache = PathCache::with_capacity(4);
        let first = cache.compile("$.a.b").unwrap();
        let second = cache.compile("$.a.b").unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let diagnostic = cache.compile("$.a[").unwrap_err();
        assert!(diagnostic.to_string().contains("invalid SQL/JSON path"));
    }
}
