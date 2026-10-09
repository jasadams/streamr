//! The four ANSI SQL/JSON scalar kernels: JSON_VALUE, JSON_QUERY,
//! JSON_EXISTS and JSON_OBJECT.
//!
//! Registered through `functions::register_all`, so the planner schema
//! provider and the worker physical-plan registry (`new_registry`) always
//! resolve the same implementations. Literal SQL/JSON paths are compiled
//! through [`crate::sql_json::path`] — validated at plan time by the AST
//! lowering and memoized per process here, so a path is never reparsed per
//! row.
//!
//! Error model: malformed documents, missing paths and incompatible scalar
//! conversions obey each function's standard default (`NULL ON ERROR`;
//! `FALSE ON ERROR` for JSON_EXISTS). Engine bound violations (document
//! size, path expansion, output size) are hard
//! [`DataFusionError::ResourcesExhausted`] failures, never data-conversion
//! results.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, StringArray};
use arrow_schema::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use serde_json::{Number, Value};

use super::path::{CompiledPath, MAX_DOCUMENT_BYTES, MAX_OUTPUT_BYTES, PathItem};

/// SQL/JSON `JSON_VALUE` with a fixed `RETURNING` type.
///
/// DataFusion derives an expression's type from `return_type(&arg_types)`,
/// where argument *values* are invisible, so each returning type is a
/// distinct registered function name (`json_value`, `json_value_boolean`,
/// `json_value_double`) selected by the AST lowering. That keeps the return
/// type structural and makes it survive name-based physical-plan
/// serialization unchanged.
pub struct JsonValueFunc {
    returning: Returning,
    signature: Signature,
}

/// SQL/JSON `JSON_QUERY`.
pub struct JsonQueryFunc {
    signature: Signature,
}

/// SQL/JSON `JSON_EXISTS`.
pub struct JsonExistsFunc {
    signature: Signature,
}

/// The `[key, format_json]` pair list parsed from a JSON_OBJECT metadata
/// literal, shared because the literal is memoized.
type ObjectPairs = Arc<Vec<(String, bool)>>;

/// SQL/JSON `JSON_OBJECT`.
///
/// Arguments are the pair-metadata literal (a JSON array of
/// `[key, format_json]` entries, carrying key order and FORMAT JSON flags so
/// both survive name-based serialization) followed by one value expression
/// per pair.
pub struct JsonObjectFunc {
    signature: Signature,
    /// Memoized parse of pair-metadata literals, keyed by the literal text.
    pairs: RwLock<HashMap<String, ObjectPairs>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Returning {
    Varchar,
    Boolean,
    DoublePrecision,
}

impl Returning {
    fn data_type(self) -> DataType {
        match self {
            Returning::Varchar => DataType::Utf8,
            Returning::Boolean => DataType::Boolean,
            Returning::DoublePrecision => DataType::Float64,
        }
    }

    fn function_name(self) -> &'static str {
        match self {
            Returning::Varchar => "json_value",
            Returning::Boolean => "json_value_boolean",
            Returning::DoublePrecision => "json_value_double",
        }
    }
}

fn extraction_signature() -> Signature {
    Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable)
}

fn document_too_large(function: &str, bytes: usize) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "{function} document of {bytes} bytes exceeds the maximum accepted document size of {MAX_DOCUMENT_BYTES} bytes"
    ))
}

fn output_too_large(function: &str, bytes: usize) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "{function} produced {bytes} bytes, exceeding the maximum output size of {MAX_OUTPUT_BYTES} bytes"
    ))
}

/// Resolve the compiled path for one invocation.
///
/// The lowering always emits a literal path; the per-process path cache makes
/// execution compile each distinct literal at most once rather than once per
/// row. A non-literal path (only reachable by invoking the registered
/// function directly) and a null path are precise errors, never silent
/// fallbacks.
fn compiled_path(path: &ColumnarValue, function: &str) -> Result<Arc<CompiledPath>> {
    match path {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
            super::path::compile_literal_path(s)
        }
        ColumnarValue::Scalar(_) => Err(DataFusionError::Execution(format!(
            "the path argument to {function} cannot be null and must be UTF8 text"
        ))),
        ColumnarValue::Array(_) => Err(DataFusionError::Execution(format!(
            "the path argument to {function} must be a string literal"
        ))),
    }
}

fn row_str(array: &ArrayRef, row: usize) -> Result<Option<&str>> {
    let strings = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("SQL/JSON document argument is not UTF8".into())
        })?;
    if strings.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(strings.value(row)))
    }
}

/// Per-row outcome of document parsing + path evaluation, before the parsed
/// document is dropped. Kernels consume the items through
/// [`with_extraction`] so borrowed items never outlive the parsed value.
enum Extraction<'a> {
    /// The document was SQL NULL.
    NullDocument,
    /// The document parsed and the path produced these items (possibly none).
    Items(Vec<PathItem<'a>>),
    /// The document failed to parse (malformed JSON).
    MalformedDocument,
}

/// Parse one document, evaluate the path, and fold the outcome through `f`
/// while the parsed document is still alive. The document-size bound and the
/// expansion bound (an engine failure, never a data-conversion result) are
/// enforced here.
fn with_extraction<T>(
    document: Option<&str>,
    path: &CompiledPath,
    function: &str,
    f: impl FnOnce(Extraction<'_>) -> Result<T>,
) -> Result<T> {
    let Some(document) = document else {
        return f(Extraction::NullDocument);
    };
    if document.len() > MAX_DOCUMENT_BYTES {
        return Err(document_too_large(function, document.len()));
    }
    match serde_json::from_str::<Value>(document) {
        Ok(value) => {
            let items = path.evaluate(&value).map_err(DataFusionError::from)?;
            f(Extraction::Items(items))
        }
        Err(_) => f(Extraction::MalformedDocument),
    }
}

/// The scalar view of one matched path item.
enum ScalarJson<'a> {
    Str(&'a str),
    Number(&'a Number),
    Unsigned(u64),
    Bool(bool),
    /// An object or array where a scalar is required.
    NonScalar,
}

fn item_scalar<'a>(item: &'a PathItem<'a>) -> Option<ScalarJson<'a>> {
    match item {
        PathItem::Value(Value::Null) => None,
        PathItem::Value(Value::String(s)) => Some(ScalarJson::Str(s)),
        PathItem::Value(Value::Number(n)) => Some(ScalarJson::Number(n)),
        PathItem::Value(Value::Bool(b)) => Some(ScalarJson::Bool(*b)),
        PathItem::Value(_) => Some(ScalarJson::NonScalar),
        PathItem::Str(s) => Some(ScalarJson::Str(s)),
        PathItem::Id(id) => Some(ScalarJson::Unsigned(*id)),
        PathItem::KeyValue { .. } => Some(ScalarJson::NonScalar),
    }
}

/// JSON_VALUE's per-item conversion under the default
/// NULL ON EMPTY / NULL ON ERROR behavior.
///
/// `Ok(None)` is SQL NULL: a JSON null item, an incompatible conversion, or
/// an object/array where a scalar is required (never stringified).
fn convert_value_item(item: &PathItem<'_>, returning: Returning) -> Option<ScalarValue> {
    let Some(scalar) = item_scalar(item) else {
        // A present JSON null is SQL NULL.
        return None;
    };
    match (returning, scalar) {
        // VARCHAR: JSON scalars convert to their character representation.
        (Returning::Varchar, ScalarJson::Str(s)) => Some(ScalarValue::Utf8(Some(s.to_string()))),
        (Returning::Varchar, ScalarJson::Number(n)) => Some(ScalarValue::Utf8(Some(n.to_string()))),
        (Returning::Varchar, ScalarJson::Unsigned(id)) => {
            Some(ScalarValue::Utf8(Some(id.to_string())))
        }
        (Returning::Varchar, ScalarJson::Bool(b)) => Some(ScalarValue::Utf8(Some(
            if b { "true" } else { "false" }.to_string(),
        ))),
        (Returning::Varchar, ScalarJson::NonScalar) => None,
        // BOOLEAN: true/false pass through; strings use the standard Boolean
        // literal forms; numbers are never truthy values.
        (Returning::Boolean, ScalarJson::Bool(b)) => Some(ScalarValue::Boolean(Some(b))),
        (Returning::Boolean, ScalarJson::Str(s)) => {
            parse_boolean_literal(s).map(|b| ScalarValue::Boolean(Some(b)))
        }
        (Returning::Boolean, ScalarJson::Number(_))
        | (Returning::Boolean, ScalarJson::Unsigned(_))
        | (Returning::Boolean, ScalarJson::NonScalar) => None,
        // DOUBLE PRECISION: numbers and numeric strings convert; booleans
        // and non-scalars are incompatible.
        (Returning::DoublePrecision, ScalarJson::Number(n)) => {
            n.as_f64().map(|f| ScalarValue::Float64(Some(f)))
        }
        (Returning::DoublePrecision, ScalarJson::Unsigned(id)) => {
            Some(ScalarValue::Float64(Some(id as f64)))
        }
        (Returning::DoublePrecision, ScalarJson::Str(s)) => parse_numeric_string(s),
        (Returning::DoublePrecision, ScalarJson::Bool(_))
        | (Returning::DoublePrecision, ScalarJson::NonScalar) => None,
    }
}

/// The standard SQL Boolean string forms, case-insensitive, whitespace
/// trimmed. Anything else (including numeric strings) is incompatible.
fn parse_boolean_literal(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// Numeric-string conversion for `RETURNING DOUBLE PRECISION`, matching the
/// Arrow UTF8→Float64 cast behavior (surrounding whitespace accepted,
/// anything unparseable is incompatible).
fn parse_numeric_string(raw: &str) -> Option<ScalarValue> {
    raw.trim()
        .parse::<f64>()
        .ok()
        .map(|f| ScalarValue::Float64(Some(f)))
}

/// Serialize one matched item for JSON_QUERY: compact JSON, no wrapper.
/// A matched JSON null item serializes to the four-character text `null`,
/// distinct from SQL NULL (which means no match).
fn serialize_query_item(item: &PathItem<'_>) -> Result<String> {
    let text = match item {
        PathItem::Value(value) => serde_json::to_string(value).map_err(|error| {
            DataFusionError::Internal(format!("JSON_QUERY serialization failed: {error}"))
        })?,
        PathItem::Str(s) => serde_json::to_string(s).map_err(|error| {
            DataFusionError::Internal(format!("JSON_QUERY serialization failed: {error}"))
        })?,
        PathItem::Id(id) => id.to_string(),
        PathItem::KeyValue { key, value, id } => {
            let key = serde_json::to_string(key).map_err(|error| {
                DataFusionError::Internal(format!("JSON_QUERY serialization failed: {error}"))
            })?;
            let value = serde_json::to_string(value).map_err(|error| {
                DataFusionError::Internal(format!("JSON_QUERY serialization failed: {error}"))
            })?;
            format!("{{\"key\":{key},\"value\":{value},\"id\":{id}}}")
        }
    };
    if text.len() > MAX_OUTPUT_BYTES {
        return Err(output_too_large("JSON_QUERY", text.len()));
    }
    Ok(text)
}

impl JsonValueFunc {
    pub fn new(returning: Returning) -> Self {
        Self {
            returning,
            signature: extraction_signature(),
        }
    }

    pub fn returning(&self) -> Returning {
        self.returning
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        const FUNCTION: &str = "JSON_VALUE";
        let path = compiled_path(&args.args[1], FUNCTION)?;
        let rows = args.number_rows;
        let documents = args.args[0].clone().into_array(rows)?;
        match self.returning {
            Returning::Varchar => {
                let mut builder: Vec<Option<String>> = Vec::with_capacity(rows);
                for row in 0..rows {
                    builder.push(self.value_one_row(&documents, row, &path)?);
                }
                Ok(ColumnarValue::Array(
                    Arc::new(StringArray::from(builder)) as ArrayRef
                ))
            }
            Returning::Boolean => {
                let mut builder: Vec<Option<bool>> = Vec::with_capacity(rows);
                for row in 0..rows {
                    builder.push(self.value_one_row(&documents, row, &path)?);
                }
                Ok(ColumnarValue::Array(
                    Arc::new(BooleanArray::from(builder)) as ArrayRef
                ))
            }
            Returning::DoublePrecision => {
                let mut builder: Vec<Option<f64>> = Vec::with_capacity(rows);
                for row in 0..rows {
                    builder.push(self.value_one_row(&documents, row, &path)?);
                }
                Ok(ColumnarValue::Array(
                    Arc::new(Float64Array::from(builder)) as ArrayRef
                ))
            }
        }
    }

    fn value_one_row<T: FromJsonRow>(
        &self,
        documents: &ArrayRef,
        row: usize,
        path: &CompiledPath,
    ) -> Result<Option<T>> {
        let document = row_str(documents, row)?;
        with_extraction(document, path, "JSON_VALUE", |extraction| {
            let items = match extraction {
                Extraction::NullDocument | Extraction::MalformedDocument => return Ok(None),
                Extraction::Items(items) => items,
            };
            match items.len() {
                0 => Ok(None),
                1 => match convert_value_item(&items[0], self.returning) {
                    Some(scalar) => T::from_scalar(scalar),
                    None => Ok(None),
                },
                // Multiple matches are the default error branch: SQL NULL,
                // never first-item selection.
                _ => Ok(None),
            }
        })
    }
}

/// Convert a per-row converted scalar into the builder element type.
trait FromJsonRow: Sized {
    fn from_scalar(scalar: ScalarValue) -> Result<Option<Self>>;
}

impl FromJsonRow for String {
    fn from_scalar(scalar: ScalarValue) -> Result<Option<Self>> {
        match scalar {
            ScalarValue::Utf8(Some(s)) => Ok(Some(s)),
            ScalarValue::Utf8(None) => Ok(None),
            other => Err(DataFusionError::Internal(format!(
                "JSON_VALUE VARCHAR conversion produced {other:?}"
            ))),
        }
    }
}

impl FromJsonRow for bool {
    fn from_scalar(scalar: ScalarValue) -> Result<Option<Self>> {
        match scalar {
            ScalarValue::Boolean(b) => Ok(b),
            other => Err(DataFusionError::Internal(format!(
                "JSON_VALUE BOOLEAN conversion produced {other:?}"
            ))),
        }
    }
}

impl FromJsonRow for f64 {
    fn from_scalar(scalar: ScalarValue) -> Result<Option<Self>> {
        match scalar {
            ScalarValue::Float64(f) => Ok(f),
            other => Err(DataFusionError::Internal(format!(
                "JSON_VALUE DOUBLE PRECISION conversion produced {other:?}"
            ))),
        }
    }
}

impl JsonQueryFunc {
    pub fn new() -> Self {
        Self {
            signature: extraction_signature(),
        }
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        const FUNCTION: &str = "JSON_QUERY";
        let path = compiled_path(&args.args[1], FUNCTION)?;
        let rows = args.number_rows;
        let documents = args.args[0].clone().into_array(rows)?;
        let mut builder: Vec<Option<String>> = Vec::with_capacity(rows);
        for row in 0..rows {
            let document = row_str(&documents, row)?;
            let text = with_extraction(document, &path, FUNCTION, |extraction| {
                Ok(match extraction {
                    Extraction::NullDocument | Extraction::MalformedDocument => None,
                    Extraction::Items(items) => match items.len() {
                        0 => None,
                        1 => Some(serialize_query_item(&items[0])?),
                        // Multiple matches: default NULL ON ERROR.
                        _ => None,
                    },
                })
            })?;
            builder.push(text);
        }
        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(builder)) as ArrayRef
        ))
    }
}

impl Default for JsonQueryFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonExistsFunc {
    pub fn new() -> Self {
        Self {
            signature: extraction_signature(),
        }
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        const FUNCTION: &str = "JSON_EXISTS";
        let path = compiled_path(&args.args[1], FUNCTION)?;
        let rows = args.number_rows;
        let documents = args.args[0].clone().into_array(rows)?;
        let mut builder: Vec<Option<bool>> = Vec::with_capacity(rows);
        for row in 0..rows {
            let document = row_str(&documents, row)?;
            let present = with_extraction(document, &path, FUNCTION, |extraction| {
                Ok(match extraction {
                    // SQL-null document → SQL NULL.
                    Extraction::NullDocument => None,
                    // Default FALSE ON ERROR: a malformed document is FALSE.
                    Extraction::MalformedDocument => Some(false),
                    // Any item — including a JSON null item — is presence.
                    Extraction::Items(items) => Some(!items.is_empty()),
                })
            })?;
            builder.push(present);
        }
        Ok(ColumnarValue::Array(
            Arc::new(BooleanArray::from(builder)) as ArrayRef
        ))
    }
}

impl Default for JsonExistsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObjectFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
            pairs: RwLock::new(HashMap::new()),
        }
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        const FUNCTION: &str = "JSON_OBJECT";
        let rows = args.number_rows;
        if args.args.is_empty() {
            return Err(DataFusionError::Internal(
                "json_object requires at least the pair-metadata argument".into(),
            ));
        }
        let meta_column = args.args[0].clone().into_array(rows)?;
        // Evaluate every value column once, not once per row.
        let values: Vec<ArrayRef> = args.args[1..]
            .iter()
            .map(|arg| arg.clone().into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let mut builder: Vec<Option<String>> = Vec::with_capacity(rows);
        for row in 0..rows {
            let meta_text = row_str(&meta_column, row)?.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{FUNCTION} pair metadata cannot be null; keys are string literals"
                ))
            })?;
            let pairs = self.pairs_for(meta_text)?;
            if pairs.len() != values.len() {
                return Err(DataFusionError::Internal(format!(
                    "{FUNCTION} pair metadata describes {} pairs but {} values were supplied",
                    pairs.len(),
                    values.len()
                )));
            }
            let mut output = String::from("{");
            for (index, (key, format_json)) in pairs.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).map_err(|error| {
                    DataFusionError::Internal(format!(
                        "{FUNCTION} key serialization failed: {error}"
                    ))
                })?);
                output.push(':');
                let value = ScalarValue::try_from_array(&values[index], row)?;
                output.push_str(&object_value_text(&value, *format_json, key)?);
                if output.len() > MAX_OUTPUT_BYTES {
                    return Err(output_too_large(FUNCTION, output.len()));
                }
            }
            output.push('}');
            builder.push(Some(output));
        }
        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(builder)) as ArrayRef
        ))
    }

    fn pairs_for(&self, meta: &str) -> Result<Arc<Vec<(String, bool)>>> {
        if let Some(pairs) = self.pairs.read().unwrap().get(meta) {
            return Ok(Arc::clone(pairs));
        }
        let parsed: Vec<(String, bool)> = serde_json::from_str(meta).map_err(|error| {
            DataFusionError::Execution(format!("invalid JSON_OBJECT pair metadata: {error}"))
        })?;
        let mut seen = HashSet::with_capacity(parsed.len());
        for (key, _) in &parsed {
            if !seen.insert(key.as_str()) {
                return Err(DataFusionError::Execution(format!(
                    "duplicate JSON_OBJECT key {key:?}; JSON_OBJECT keys must be unique"
                )));
            }
        }
        let pairs = Arc::new(parsed);
        let mut cache = self.pairs.write().unwrap();
        if cache.len() >= 4096 {
            cache.clear();
        }
        cache.insert(meta.to_string(), Arc::clone(&pairs));
        Ok(pairs)
    }
}

impl Default for JsonObjectFunc {
    fn default() -> Self {
        Self::new()
    }
}

/// Render one JSON_OBJECT value.
///
/// Default NULL ON NULL: a SQL-null value (bare `NULL` or any typed null) is
/// included as JSON null. Without FORMAT JSON, VARCHAR values are quoted and
/// escaped, booleans and numbers render per JSON, and unsupported types are
/// construction errors. With FORMAT JSON the standard's character-operand
/// rule applies: only character values are accepted (validated as one
/// complete JSON value and embedded whole); any other non-null value is a
/// hard construction error, never a silent drop or a re-typed embed.
fn object_value_text(value: &ScalarValue, format_json: bool, key: &str) -> Result<String> {
    let is_null = matches!(
        value,
        ScalarValue::Null
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Boolean(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None)
    );
    if is_null {
        return Ok("null".to_string());
    }
    if format_json {
        return match value {
            ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => {
                validate_and_embed_json(text, key)
            }
            other => Err(DataFusionError::Execution(format!(
                "FORMAT JSON for JSON_OBJECT key {key:?} requires a character value, not {}",
                non_null_kind(other)
            ))),
        };
    }
    match value {
        ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => {
            serde_json::to_string(text).map_err(|error| {
                DataFusionError::Internal(format!(
                    "JSON_OBJECT string serialization failed: {error}"
                ))
            })
        }
        ScalarValue::Boolean(Some(b)) => Ok(if *b { "true" } else { "false" }.to_string()),
        ScalarValue::Int8(Some(v)) => Ok(v.to_string()),
        ScalarValue::Int16(Some(v)) => Ok(v.to_string()),
        ScalarValue::Int32(Some(v)) => Ok(v.to_string()),
        ScalarValue::Int64(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Ok(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Ok(v.to_string()),
        ScalarValue::Float32(Some(v)) => json_f64(*v as f64, key),
        ScalarValue::Float64(Some(v)) => json_f64(*v, key),
        other => Err(DataFusionError::Execution(format!(
            "unsupported JSON_OBJECT value type for key {key:?}: {other}"
        ))),
    }
}

/// A short label for a non-null scalar kind, used in FORMAT JSON errors.
fn non_null_kind(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Boolean(_) => "a boolean",
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Float32(_)
        | ScalarValue::Float64(_) => "a number",
        _ => "a non-character value",
    }
}

fn json_f64(value: f64, key: &str) -> Result<String> {
    Number::from_f64(value)
        .map(|number| number.to_string())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "JSON_OBJECT key {key:?} received a non-finite floating-point value, which is not valid JSON"
            ))
        })
}

fn validate_and_embed_json(text: &str, key: &str) -> Result<String> {
    match serde_json::from_str::<Value>(text) {
        Ok(_) => Ok(text.to_string()),
        Err(error) => Err(DataFusionError::Execution(format!(
            "FORMAT JSON value for JSON_OBJECT key {key:?} is not a complete JSON value: {error}"
        ))),
    }
}

impl ScalarUDFImpl for JsonValueFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.returning.function_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.returning.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke(args)
    }
}

impl ScalarUDFImpl for JsonQueryFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_query"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke(args)
    }
}

impl ScalarUDFImpl for JsonExistsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_exists"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke(args)
    }
}

impl ScalarUDFImpl for JsonObjectFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(
                "json_object requires at least the pair-metadata argument".into(),
            ));
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke(args)
    }
}

impl std::fmt::Debug for JsonValueFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonValueFunc({:?})", self.returning)
    }
}

impl std::fmt::Debug for JsonQueryFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonQueryFunc")
    }
}

impl std::fmt::Debug for JsonExistsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonExistsFunc")
    }
}

impl std::fmt::Debug for JsonObjectFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonObjectFunc")
    }
}

/// Registered UDF instances for `functions::register_all`.
pub fn json_value() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonValueFunc::new(
        Returning::Varchar,
    )))
}

pub fn json_value_boolean() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonValueFunc::new(
        Returning::Boolean,
    )))
}

pub fn json_value_double() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonValueFunc::new(
        Returning::DoublePrecision,
    )))
}

pub fn json_query() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonQueryFunc::new()))
}

pub fn json_exists() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonExistsFunc::new()))
}

pub fn json_object() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonObjectFunc::new()))
}

/// The reserved builtin names. User UDF registration must never shadow these
/// (see `ArroyoSchemaProvider::add_rust_udf` / `add_python_udf`).
pub const RESERVED_SQL_JSON_UDF_NAMES: [&str; 6] = [
    "json_value",
    "json_value_boolean",
    "json_value_double",
    "json_query",
    "json_exists",
    "json_object",
];

pub fn is_reserved_sql_json_udf_name(name: &str) -> bool {
    RESERVED_SQL_JSON_UDF_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    fn invoke(udf: &ScalarUDF, args: Vec<ScalarValue>) -> Result<ColumnarValue> {
        let arg_types: Vec<DataType> = args.iter().map(|a| a.data_type()).collect();
        let rows = 1;
        // Keep the arguments as scalars: the SQL/JSON kernels require a
        // scalar literal path, matching what the AST lowering always emits.
        let args = args.into_iter().map(ColumnarValue::Scalar).collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: (0..arg_types.len())
                .map(|_| Arc::new(Field::new("arg", DataType::Utf8, true)))
                .collect(),
            number_rows: rows,
            return_field: Arc::new(Field::new("result", udf.return_type(&arg_types)?, true)),
        })
    }

    fn utf8(value: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(value.to_string()))
    }

    fn one_string(result: ColumnarValue) -> Option<String> {
        let array = result.into_array(1).unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        if strings.is_null(0) {
            None
        } else {
            Some(strings.value(0).to_string())
        }
    }

    fn one_bool(result: ColumnarValue) -> Option<bool> {
        let array = result.into_array(1).unwrap();
        let booleans = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        if booleans.is_null(0) {
            None
        } else {
            Some(booleans.value(0))
        }
    }

    fn one_f64(result: ColumnarValue) -> Option<f64> {
        let array = result.into_array(1).unwrap();
        let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
        if floats.is_null(0) {
            None
        } else {
            Some(floats.value(0))
        }
    }

    const P1: &str = r#"{"traits":{"name":null,"quality_score":0,"steam_wishlisted":false,"linked_accounts":{"discord":{"external_id":"d1"}}},"identifiers":[{"identity_type":"email","value":"a@example.test"}]}"#;

    #[test]
    fn value_missing_json_null_and_scalars() {
        let udf = json_value();
        // Missing path → SQL NULL.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.absent")]).unwrap()),
            None
        );
        // Present JSON null → SQL NULL.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.name")]).unwrap()),
            None
        );
        // Numeric zero preserved as VARCHAR "0".
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.quality_score")]).unwrap()),
            Some("0".to_string())
        );
        // JSON false preserved as VARCHAR "false".
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.steam_wishlisted")]).unwrap()),
            Some("false".to_string())
        );
        // Empty string preserved.
        let empty = r#"{"traits":{"name":""}}"#;
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(empty), utf8("$.traits.name")]).unwrap()),
            Some(String::new())
        );
        // SQL-null document → SQL NULL.
        assert_eq!(
            one_string(invoke(&udf, vec![ScalarValue::Utf8(None), utf8("$.a")]).unwrap()),
            None
        );
        // Malformed document → NULL under NULL ON ERROR.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8("{"), utf8("$.a")]).unwrap()),
            None
        );
        // Object where a scalar is required → NULL, never stringified.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.linked_accounts")]).unwrap()),
            None
        );
    }

    #[test]
    fn value_email_filter_and_two_item_error_branch() {
        let udf = json_value();
        assert_eq!(
            one_string(
                invoke(
                    &udf,
                    vec![
                        utf8(P1),
                        utf8("$.identifiers[*] ? (@.identity_type == \"email\").value")
                    ]
                )
                .unwrap()
            ),
            Some("a@example.test".to_string())
        );
        // Two matching identities: the error branch under NULL ON ERROR —
        // SQL NULL, never first-item selection.
        let two = r#"{"identifiers":[{"identity_type":"email","value":"a"},{"identity_type":"email","value":"b"}]}"#;
        assert_eq!(
            one_string(
                invoke(
                    &udf,
                    vec![
                        utf8(two),
                        utf8("$.identifiers[*] ? (@.identity_type == \"email\").value")
                    ]
                )
                .unwrap()
            ),
            None
        );
    }

    #[test]
    fn value_returning_conversions() {
        let boolean = json_value_boolean();
        let double = json_value_double();
        let varchar = json_value();
        // JSON false → FALSE.
        assert_eq!(
            one_bool(invoke(&boolean, vec![utf8(P1), utf8("$.traits.steam_wishlisted")]).unwrap()),
            Some(false)
        );
        // Numeric zero → 0.0.
        assert_eq!(
            one_f64(invoke(&double, vec![utf8(P1), utf8("$.traits.quality_score")]).unwrap()),
            Some(0.0)
        );
        // Standard Boolean string forms.
        for (text, expected) in [
            ("true", Some(true)),
            ("TRUE", Some(true)),
            (" t ", Some(true)),
            ("yes", Some(true)),
            ("on", Some(true)),
            ("1", Some(true)),
            ("false", Some(false)),
            ("N", Some(false)),
            ("off", Some(false)),
            ("0", Some(false)),
            ("", None),
            ("maybe", None),
            ("2", None),
        ] {
            let doc = format!("{{\"v\":\"{text}\"}}");
            assert_eq!(
                one_bool(invoke(&boolean, vec![utf8(&doc), utf8("$.v")]).unwrap()),
                expected,
                "boolean literal {text:?}"
            );
        }
        // Numbers are never truthy: RETURNING BOOLEAN of 0 is incompatible.
        assert_eq!(
            one_bool(invoke(&boolean, vec![utf8(r#"{"v":0}"#), utf8("$.v")]).unwrap()),
            None
        );
        assert_eq!(
            one_bool(invoke(&boolean, vec![utf8(r#"{"v":1}"#), utf8("$.v")]).unwrap()),
            None
        );
        // Numeric strings convert to DOUBLE PRECISION.
        assert_eq!(
            one_f64(invoke(&double, vec![utf8(r#"{"v":"1.5"}"#), utf8("$.v")]).unwrap()),
            Some(1.5)
        );
        assert_eq!(
            one_f64(invoke(&double, vec![utf8(r#"{"v":" 2e3 "}"#), utf8("$.v")]).unwrap()),
            Some(2000.0)
        );
        // Incompatible strings → NULL.
        assert_eq!(
            one_f64(invoke(&double, vec![utf8(r#"{"v":"abc"}"#), utf8("$.v")]).unwrap()),
            None
        );
        // Booleans are incompatible with DOUBLE PRECISION.
        assert_eq!(
            one_f64(invoke(&double, vec![utf8(r#"{"v":true}"#), utf8("$.v")]).unwrap()),
            None
        );
        // Escaped strings survive round trips.
        assert_eq!(
            one_string(invoke(&varchar, vec![utf8(r#"{"v":"a\"b\nc"}"#), utf8("$.v")]).unwrap()),
            Some("a\"b\nc".to_string())
        );
        // Nested objects.
        assert_eq!(
            one_string(
                invoke(
                    &varchar,
                    vec![
                        utf8(P1),
                        utf8("$.traits.linked_accounts.discord.external_id")
                    ]
                )
                .unwrap()
            ),
            Some("d1".to_string())
        );
        // Arrays: JSON_VALUE of an array is the error branch → NULL.
        assert_eq!(
            one_string(invoke(&varchar, vec![utf8(r#"{"v":[1,2]}"#), utf8("$.v")]).unwrap()),
            None
        );
    }

    #[test]
    fn query_null_item_is_text_null_not_sql_null() {
        let udf = json_query();
        // Matched JSON null serializes to the four-character text "null".
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.name")]).unwrap()),
            Some("null".to_string())
        );
        // A matched object serializes without a wrapper.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.linked_accounts")]).unwrap()),
            Some(r#"{"discord":{"external_id":"d1"}}"#.to_string())
        );
        // No match → SQL NULL.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(P1), utf8("$.traits.absent")]).unwrap()),
            None
        );
        // SQL-null document → SQL NULL.
        assert_eq!(
            one_string(invoke(&udf, vec![ScalarValue::Utf8(None), utf8("$.a")]).unwrap()),
            None
        );
        // Malformed document → SQL NULL under NULL ON ERROR.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8("not json"), utf8("$.a")]).unwrap()),
            None
        );
        // A single matched array element serializes; a multi-item sequence is
        // the error branch → SQL NULL.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(r#"{"v":[1]}"#), utf8("$.v[*]")]).unwrap()),
            Some("1".to_string())
        );
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(r#"{"xs":[1,2]}"#), utf8("$.xs[*]")]).unwrap()),
            None
        );
    }

    #[test]
    fn exists_presence_semantics() {
        let udf = json_exists();
        // Present JSON null → TRUE.
        assert_eq!(
            one_bool(invoke(&udf, vec![utf8(P1), utf8("$.traits.name")]).unwrap()),
            Some(true)
        );
        // Explicit null test: TRUE only for a present JSON null.
        assert_eq!(
            one_bool(invoke(&udf, vec![utf8(P1), utf8("$.traits.name ? (@ == null)")]).unwrap()),
            Some(true)
        );
        assert_eq!(
            one_bool(invoke(&udf, vec![utf8("{}"), utf8("$.traits.name ? (@ == null)")]).unwrap()),
            Some(false)
        );
        // No matching item → FALSE.
        assert_eq!(
            one_bool(invoke(&udf, vec![utf8("{}"), utf8("$.traits.name")]).unwrap()),
            Some(false)
        );
        // SQL-null document → SQL NULL.
        assert_eq!(
            one_bool(invoke(&udf, vec![ScalarValue::Utf8(None), utf8("$.a")]).unwrap()),
            None
        );
        // Malformed document → FALSE under FALSE ON ERROR.
        assert_eq!(
            one_bool(invoke(&udf, vec![utf8("{"), utf8("$.a")]).unwrap()),
            Some(false)
        );
    }

    #[test]
    fn object_construction_matrix() {
        let udf = json_object();
        // Empty object: JSON_OBJECT() → {}.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8("[]")]).unwrap()),
            Some("{}".to_string())
        );
        // Order preserved; VARCHAR quoted; boolean/numeric per JSON; SQL null
        // included as JSON null (NULL ON NULL default).
        let meta = r#"[["source",false],["medium",false],["n",false],["b",false],["z",false]]"#;
        assert_eq!(
            one_string(
                invoke(
                    &udf,
                    vec![
                        utf8(meta),
                        utf8("google"),
                        utf8(""),
                        ScalarValue::Int64(Some(7)),
                        ScalarValue::Boolean(Some(false)),
                        ScalarValue::Utf8(None),
                    ]
                )
                .unwrap()
            ),
            Some(r#"{"source":"google","medium":"","n":7,"b":false,"z":null}"#.to_string())
        );
        // FORMAT JSON embeds the validated document whole.
        let meta = r#"[["accounts",true]]"#;
        assert_eq!(
            one_string(
                invoke(
                    &udf,
                    vec![utf8(meta), utf8(r#"{"discord":{"external_id":"d1"}}"#),]
                )
                .unwrap()
            ),
            Some(r#"{"accounts":{"discord":{"external_id":"d1"}}}"#.to_string())
        );
        // Invalid FORMAT JSON is a hard construction error, not a drop.
        let error = invoke(&udf, vec![utf8(meta), utf8("{not json")]).unwrap_err();
        assert!(
            error.to_string().contains("FORMAT JSON") && error.to_string().contains("accounts"),
            "{error}"
        );
        // Duplicate keys never overwrite: hard error.
        let error = invoke(
            &udf,
            vec![utf8(r#"[["a",false],["a",false]]"#), utf8("1"), utf8("2")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("duplicate"), "{error}");
        // Non-string FORMAT JSON value is an error.
        let error = invoke(
            &udf,
            vec![utf8(r#"[["a",true]]"#), ScalarValue::Boolean(Some(true))],
        )
        .unwrap_err();
        assert!(error.to_string().contains("FORMAT JSON"), "{error}");
        // Numeric values under FORMAT JSON follow the same character-operand
        // rule as booleans.
        let error = invoke(
            &udf,
            vec![utf8(r#"[["a",true]]"#), ScalarValue::Float64(Some(1.5))],
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("FORMAT JSON") && error.to_string().contains("number"),
            "{error}"
        );
    }

    #[test]
    fn object_bare_and_typed_null_values_are_json_null() {
        let udf = json_object();
        // DataFusion plans a bare `VALUE NULL` under VariadicAny as
        // ScalarValue::Null (no coercion); it must render as JSON null, the
        // canonical NULL ON NULL spelling.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(r#"[["k",false]]"#), ScalarValue::Null]).unwrap()),
            Some(r#"{"k":null}"#.to_string())
        );
        // CAST(NULL AS BIGINT) — a typed null — is also JSON null.
        assert_eq!(
            one_string(
                invoke(
                    &udf,
                    vec![utf8(r#"[["k",false]]"#), ScalarValue::Int64(None)]
                )
                .unwrap()
            ),
            Some(r#"{"k":null}"#.to_string())
        );
        // A bare NULL under FORMAT JSON is still JSON null: NULL ON NULL
        // applies before the character-operand requirement.
        assert_eq!(
            one_string(invoke(&udf, vec![utf8(r#"[["k",true]]"#), ScalarValue::Null]).unwrap()),
            Some(r#"{"k":null}"#.to_string())
        );
    }

    #[test]
    fn object_rejects_null_metadata() {
        let udf = json_object();
        let error = invoke(&udf, vec![ScalarValue::Utf8(None)]).unwrap_err();
        assert!(error.to_string().contains("metadata"), "{error}");
    }

    #[test]
    fn null_path_argument_is_an_error() {
        let udf = json_value();
        let error = invoke(&udf, vec![utf8(P1), ScalarValue::Utf8(None)]).unwrap_err();
        assert!(error.to_string().contains("path"), "{error}");
    }

    #[test]
    fn reserved_names_are_case_insensitive() {
        assert!(is_reserved_sql_json_udf_name("json_value"));
        assert!(is_reserved_sql_json_udf_name("JSON_OBJECT"));
        assert!(!is_reserved_sql_json_udf_name("my_json_value"));
    }
}
