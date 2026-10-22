//! Gated operation/scan/lifetime records; no retained key history or task.
use arroyo_state::live::{StateNamespace, StateSnapshot};
use serde_json::{Value, json};
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
static ENABLED: OnceLock<bool> = OnceLock::new();
static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);
pub(super) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("STREAMR_LIVE_SCAN_DIAGNOSTICS").as_deref() == Ok("1"))
}
fn emit(value: Value) {
    eprintln!("STREAMR_SCAN_DIAGNOSTIC {value}");
}
pub(super) struct ScopeProbe {
    pub(super) id: u64,
    namespace: StateNamespace,
    snapshot_ptr: Option<usize>,
}
impl ScopeProbe {
    pub(super) fn new(
        namespace: &StateNamespace,
        snapshot: Option<&StateSnapshot>,
    ) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let probe = Self {
            id: NEXT_SCOPE.fetch_add(1, Ordering::Relaxed),
            namespace: namespace.clone(),
            snapshot_ptr: snapshot.map(|s| std::sync::Arc::as_ptr(&s.0) as *const () as usize),
        };
        probe.event("scope_begin");
        Some(probe)
    }
    pub(super) fn event(&self, event: &str) {
        emit(
            json!({"event":event,"scope_id":self.id,"namespace":format!("{:?}",self.namespace),"snapshot_ptr":self.snapshot_ptr}),
        );
    }
    pub(super) fn first(
        &self,
        reason: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        end: Option<&[u8]>,
    ) {
        emit(
            json!({"event":"first_prefix_request","scope_id":self.id,"namespace":format!("{:?}",self.namespace),"snapshot_ptr":self.snapshot_ptr,"reason":reason,"prefix":prefix,"after":after,"end":end}),
        );
    }
    pub(super) fn mutation(&self, key: &[u8], put: bool) {
        emit(
            json!({"event":"committed_aggregate_overlay","scope_id":self.id,"namespace":format!("{:?}",self.namespace),"key":key,"put":put}),
        );
    }
}
impl Drop for ScopeProbe {
    fn drop(&mut self) {
        self.event("scope_owned_snapshot_handle_overlay_decoded_released");
    }
}

/// Declared before the output permit local so reverse local-drop order proves
/// output admission is released on success, error and cancellation.
pub(crate) struct OutputProbe {
    id: Option<u64>,
}
impl OutputProbe {
    pub(crate) fn new() -> Self {
        Self { id: None }
    }
    pub(crate) fn bind(&mut self, id: Option<u64>) {
        self.id = id;
    }
    pub(crate) fn event(&self, event: &str) {
        if let Some(id) = self.id {
            emit(json!({"event":event,"scope_id":id}));
        }
    }
}
impl Drop for OutputProbe {
    fn drop(&mut self) {
        self.event("immutable_output_permit_released");
    }
}
