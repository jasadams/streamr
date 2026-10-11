//! Limited standard LIKE_REGEX predicates. Patterns are compiled while binding
//! the logical expression and reconstructed from source by the physical codec.
//! Evaluation has no dynamic pattern cache or retained event state.
use crate::sql_json::path::{MAX_DOCUMENT_BYTES, MAX_PATH_DEPTH, MAX_PATH_LENGTH};
use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::{
    DataFusionError, Result, ScalarValue,
    tree_node::{Transformed, TreeNode},
};
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use regex::{Regex, RegexBuilder};
use std::{
    any::Any,
    hash::{Hash, Hasher},
    sync::Arc,
};

fn plan_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("LIKE_REGEX: {}", message.into()))
}

/// The approved subset deliberately excludes engine-specific escapes, inline
/// flags, lookaround, backreferences, repetition and class set operations.
fn compile(pattern: &str) -> Result<Regex> {
    if pattern.len() > MAX_PATH_LENGTH {
        return Err(plan_error(format!(
            "pattern exceeds {MAX_PATH_LENGTH} bytes"
        )));
    }
    let mut escaped = false;
    let mut class = false;
    let mut depth = 0usize;
    let mut previous = None;
    for ch in pattern.chars() {
        if escaped {
            if !".^$|()[]\\*+?{}-".contains(ch) {
                return Err(plan_error(format!("unsupported regex escape \\{ch}")));
            }
            escaped = false;
            previous = Some(ch);
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if class {
            if ch == ']' {
                class = false;
            }
            if ch == '[' || (Some(ch) == previous && "&-~".contains(ch)) {
                return Err(plan_error("unsupported character-class operator"));
            }
        } else {
            match ch {
                '[' => class = true,
                '(' => {
                    depth += 1;
                    if depth > MAX_PATH_DEPTH {
                        return Err(plan_error(format!(
                            "pattern nesting exceeds {MAX_PATH_DEPTH}"
                        )));
                    }
                }
                ')' => depth = depth.saturating_sub(1),
                '*' | '+' | '?' | '{' | '}' => {
                    return Err(plan_error(format!(
                        "unsupported advanced regex operator {ch:?}"
                    )));
                }
                _ => {}
            }
        }
        previous = Some(ch);
    }
    RegexBuilder::new(pattern)
        .size_limit(MAX_DOCUMENT_BYTES)
        .dfa_size_limit(MAX_DOCUMENT_BYTES)
        .nest_limit(MAX_PATH_DEPTH as u32)
        .build()
        .map_err(|error| plan_error(format!("invalid pattern {pattern:?}: {error}")))
}

#[derive(Debug)]
pub struct LikeRegexFunc {
    signature: Signature,
    compiled: Option<Regex>,
}
impl LikeRegexFunc {
    fn unbound() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
            compiled: None,
        }
    }
    pub fn new(pattern: &str) -> Result<Self> {
        Ok(Self {
            compiled: Some(compile(pattern)?),
            ..Self::unbound()
        })
    }
    pub fn pattern(&self) -> Option<&str> {
        self.compiled.as_ref().map(Regex::as_str)
    }
    fn matches(&self, value: Option<&str>) -> Result<Option<bool>> {
        let regex = self
            .compiled
            .as_ref()
            .ok_or_else(|| plan_error("literal pattern was not bound during planning"))?;
        value
            .map(|text| {
                if text.len() > MAX_DOCUMENT_BYTES {
                    return Err(DataFusionError::Execution(format!(
                        "LIKE_REGEX input exceeds {MAX_DOCUMENT_BYTES} bytes"
                    )));
                }
                Ok(regex.is_match(text))
            })
            .transpose()
    }
}
impl ScalarUDFImpl for LikeRegexFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "like_regex"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 || args.arg_fields[0].data_type() != &DataType::Utf8 {
            return Err(plan_error("value must be nullable UTF8"));
        }
        let Some(ScalarValue::Utf8(Some(pattern))) =
            args.scalar_arguments.get(1).and_then(|arg| *arg)
        else {
            return Err(plan_error("pattern must be a non-null UTF8 literal"));
        };
        if self.pattern().is_some_and(|compiled| compiled != pattern) {
            return Err(plan_error(
                "compiled pattern disagrees with literal argument",
            ));
        }
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            args.arg_fields[0].is_nullable(),
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(plan_error("requires value and literal pattern"));
        }
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) = &args.args[1] else {
            return Err(plan_error("pattern must be a non-null UTF8 literal"));
        };
        if self.pattern() != Some(pattern) {
            return Err(plan_error(
                "compiled pattern disagrees with literal argument",
            ));
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(self.matches(value.as_deref())?),
            )),
            ColumnarValue::Array(array) => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| plan_error("value must be nullable UTF8"))?;
                let matches = (0..values.len())
                    .map(|index| {
                        self.matches((!values.is_null(index)).then(|| values.value(index)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(matches))))
            }
            _ => Err(plan_error("value must be nullable UTF8")),
        }
    }
    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| self.pattern() == other.pattern())
    }
    fn hash_value(&self) -> u64 {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        self.name().hash(&mut hash);
        self.pattern().hash(&mut hash);
        hash.finish()
    }
}
pub fn like_regex() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(LikeRegexFunc::unbound()))
}
pub fn is_reserved_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("like_regex")
}

pub(crate) fn bind_expr(expr: Expr) -> Result<Expr> {
    expr.transform_up(|expr| {
        let Expr::ScalarFunction(mut function) = expr else {
            return Ok(Transformed::no(expr));
        };
        if function.func.inner().as_any().is::<LikeRegexFunc>()
            && function
                .func
                .inner()
                .as_any()
                .downcast_ref::<LikeRegexFunc>()
                .unwrap()
                .pattern()
                .is_none()
        {
            let Some(Expr::Literal(ScalarValue::Utf8(Some(pattern)), _)) = function.args.get(1)
            else {
                return Err(plan_error("pattern must be a non-null UTF8 literal"));
            };
            function.func = Arc::new(ScalarUDF::new_from_impl(LikeRegexFunc::new(pattern)?));
            Ok(Transformed::yes(Expr::ScalarFunction(function)))
        } else {
            Ok(Transformed::no(Expr::ScalarFunction(function)))
        }
    })
    .map(|result| result.data)
}
pub(crate) fn bind_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|plan| {
        plan.map_expressions(|expr| Ok(Transformed::yes(bind_expr(expr)?)))
    })
    .map(|result| result.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    #[test]
    fn like_regex_compiled_kernel_and_decode_bounds() {
        let regex = LikeRegexFunc::new("(^|\\.)(google\\.com|bing\\.com)$").unwrap();
        assert_eq!(regex.matches(Some("www.google.com")).unwrap(), Some(true));
        assert_eq!(regex.matches(Some("evilgoogle.com")).unwrap(), Some(false));
        assert_eq!(regex.matches(None).unwrap(), None);
        assert!(
            regex
                .matches(Some(&"x".repeat(MAX_DOCUMENT_BYTES + 1)))
                .unwrap_err()
                .to_string()
                .contains("input exceeds")
        );
        assert_eq!(regex.matches(Some("bing.com")).unwrap(), Some(true));
        let codec = crate::physical::ArroyoPhysicalExtensionCodec::default();
        assert!(
            codec
                .try_decode_udf("like_regex", &[255])
                .unwrap_err()
                .to_string()
                .contains("not UTF8")
        );
        assert!(
            codec
                .try_decode_udf("like_regex", br#""(?i)x""#)
                .unwrap_err()
                .to_string()
                .contains("unsupported advanced")
        );
        let oversized = vec![b'x'; MAX_PATH_LENGTH * 6 + 3];
        assert!(
            codec
                .try_decode_udf("like_regex", &oversized)
                .unwrap_err()
                .to_string()
                .contains("payload exceeds")
        );
        let escaped_boundary = format!("\"{}\"", "\\u0078".repeat(MAX_PATH_LENGTH));
        assert!(
            codec
                .try_decode_udf("like_regex", escaped_boundary.as_bytes())
                .is_ok()
        );
        let escaped_oversized = format!("\"{}\"", "\\u0078".repeat(MAX_PATH_LENGTH + 1));
        assert!(
            codec
                .try_decode_udf("like_regex", escaped_oversized.as_bytes())
                .unwrap_err()
                .to_string()
                .contains("payload exceeds")
        );
        let empty = ScalarUDF::new_from_impl(LikeRegexFunc::new("").unwrap());
        let mut empty_bytes = vec![];
        codec.try_encode_udf(&empty, &mut empty_bytes).unwrap();
        assert!(!empty_bytes.is_empty());
        assert!(
            codec
                .try_decode_udf("like_regex", &empty_bytes)
                .unwrap()
                .inner()
                .equals(empty.inner().as_ref())
        );
        let udf = ScalarUDF::new_from_impl(regex);
        let mut bytes = vec![];
        codec.try_encode_udf(&udf, &mut bytes).unwrap();
        let rebuilt = codec.try_decode_udf("like_regex", &bytes).unwrap();
        assert!(udf.inner().equals(rebuilt.inner().as_ref()));
        let other = ScalarUDF::new_from_impl(LikeRegexFunc::new("different").unwrap());
        assert!(!udf.inner().equals(other.inner().as_ref()));
    }
}
