//! ANSI SQL/JSON support: one compiled path implementation shared by the
//! JSON_VALUE / JSON_QUERY / JSON_EXISTS / JSON_OBJECT kernels, plus the
//! parse→plan AST lowering and (later) JSON_TABLE row expansion.
//!
//! * [`path`] — the compiled SQL/JSON path representation and evaluator.
//! * [`kernels`] — the four immutable scalar UDF implementations, registered
//!   through `functions::register_all` so the planner and the worker resolve
//!   the same implementations.
//! * [`lowering`] — rewrites the sqlparser `Expr::Json*` variants into
//!   ordinary function calls before `SqlToRel`, with planner diagnostics for
//!   invalid literal paths and duplicate JSON_OBJECT keys.

pub mod kernels;
pub mod lowering;
pub mod path;
