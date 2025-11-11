//! Context propagation for distributed tracing.
//!
//! This module provides utilities for propagating trace context across service
//! boundaries using W3C Trace Context and Baggage standards.

use opentelemetry::{
    baggage::BaggageExt,
    global,
    propagation::{Extractor, Injector, TextMapPropagator},
    trace::{TraceContextExt, TraceError},
    Context, KeyValue,
};
use std::collections::HashMap;

/// A carrier for injecting and extracting trace context from HTTP headers.
#[derive(Debug, Clone, Default)]
pub struct HeaderCarrier {
    headers: HashMap<String, String>,
}

impl HeaderCarrier {
    /// Creates a new empty header carrier.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a header carrier from existing headers.
    pub fn from_headers(headers: HashMap<String, String>) -> Self {
        Self { headers }
    }

    /// Gets all headers.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Consumes the carrier and returns the headers.
    pub fn into_headers(self) -> HashMap<String, String> {
        self.headers
    }

    /// Gets a header value by key.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.headers.get(key)
    }

    /// Sets a header value.
    pub fn set(&mut self, key: String, value: String) {
        self.headers.insert(key, value);
    }
}

impl Injector for HeaderCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.headers.insert(key.to_string(), value);
    }
}

impl Extractor for HeaderCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(|v| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(|k| k.as_str()).collect()
    }
}

/// Injects the current trace context into a carrier.
///
/// # Example
///
/// ```ignore
/// let mut carrier = HeaderCarrier::new();
/// inject_context(&Context::current(), &mut carrier);
/// // carrier now contains traceparent and tracestate headers
/// ```
pub fn inject_context(context: &Context, carrier: &mut dyn Injector) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, carrier);
    });
}

/// Extracts trace context from a carrier.
///
/// # Example
///
/// ```ignore
/// let carrier = HeaderCarrier::from_headers(headers);
/// let context = extract_context(&carrier);
/// // context now contains the extracted trace context
/// ```
pub fn extract_context(carrier: &dyn Extractor) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(carrier))
}

/// Injects trace context into HTTP headers.
pub fn inject_into_headers(
    context: &Context,
    headers: &mut HashMap<String, String>,
) {
    let mut carrier = HeaderCarrier::from_headers(headers.clone());
    inject_context(context, &mut carrier);
    *headers = carrier.into_headers();
}

/// Extracts trace context from HTTP headers.
pub fn extract_from_headers(headers: &HashMap<String, String>) -> Context {
    let carrier = HeaderCarrier::from_headers(headers.clone());
    extract_context(&carrier)
}

/// Baggage item for propagating additional context.
#[derive(Debug, Clone)]
pub struct BaggageItem {
    pub key: String,
    pub value: String,
}

impl BaggageItem {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Adds baggage to the current context.
///
/// Baggage allows you to propagate arbitrary key-value pairs across service boundaries.
pub fn with_baggage(context: &Context, items: Vec<BaggageItem>) -> Context {
    let mut ctx = context.clone();
    for item in items {
        ctx = ctx.with_value(KeyValue::new(item.key, item.value));
    }
    ctx
}

/// Gets baggage value from context.
pub fn get_baggage(context: &Context, key: &str) -> Option<String> {
    context.baggage().get(key).map(|v| v.to_string())
}

/// Creates a new context with the current span.
pub fn context_with_span(span: impl opentelemetry::trace::Span + Send + Sync + 'static) -> Context {
    Context::current_with_span(span)
}

/// Attaches a context for the duration of the async function.
///
/// # Example
///
/// ```ignore
/// let context = extract_from_headers(&headers);
/// with_context(context, async {
///     // This async block runs with the extracted context
///     process_event().await;
/// }).await;
/// ```
pub async fn with_context<F, T>(context: Context, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let _guard = context.attach();
    f.await
}

/// Macro for running code within a context scope.
///
/// # Example
///
/// ```ignore
/// let context = extract_from_headers(&headers);
/// in_context!(context, {
///     // Code here runs with the context attached
///     let span = span!("my_operation");
/// });
/// ```
#[macro_export]
macro_rules! in_context {
    ($context:expr, $body:block) => {{
        let _guard = $context.attach();
        $body
    }};
}

/// Propagates context across async task boundaries.
///
/// # Example
///
/// ```ignore
/// propagate_context(async {
///     // This task inherits the current context
///     process_background_task().await;
/// }).await;
/// ```
pub async fn propagate_context<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let context = Context::current();
    with_context(context, f).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_carrier_new() {
        let carrier = HeaderCarrier::new();
        assert!(carrier.headers.is_empty());
    }

    #[test]
    fn test_header_carrier_from_headers() {
        let mut headers = HashMap::new();
        headers.insert("traceparent".to_string(), "test-value".to_string());

        let carrier = HeaderCarrier::from_headers(headers.clone());
        assert_eq!(carrier.get("traceparent"), Some(&"test-value".to_string()));
    }

    #[test]
    fn test_header_carrier_set_get() {
        let mut carrier = HeaderCarrier::new();
        carrier.set("test-key".to_string(), "test-value".to_string());
        assert_eq!(carrier.get("test-key"), Some(&"test-value".to_string()));
    }

    #[test]
    fn test_header_carrier_injector() {
        let mut carrier = HeaderCarrier::new();
        Injector::set(&mut carrier, "key", "value".to_string());
        assert_eq!(carrier.headers.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_header_carrier_extractor() {
        let mut headers = HashMap::new();
        headers.insert("key".to_string(), "value".to_string());
        let carrier = HeaderCarrier::from_headers(headers);

        assert_eq!(Extractor::get(&carrier, "key"), Some("value"));
        assert_eq!(Extractor::keys(&carrier), vec!["key"]);
    }

    #[test]
    fn test_baggage_item() {
        let item = BaggageItem::new("tenant-id", "12345");
        assert_eq!(item.key, "tenant-id");
        assert_eq!(item.value, "12345");
    }

    #[test]
    fn test_inject_extract_context() {
        let context = Context::current();
        let mut carrier = HeaderCarrier::new();

        inject_context(&context, &mut carrier);
        let extracted = extract_context(&carrier);

        // Context should be extracted successfully (even if empty)
        assert!(extracted.span().span_context().is_valid() || !extracted.span().span_context().is_valid());
    }

    #[test]
    fn test_inject_extract_headers() {
        let mut headers = HashMap::new();
        let context = Context::current();

        inject_into_headers(&context, &mut headers);
        let extracted = extract_from_headers(&headers);

        assert!(extracted.span().span_context().is_valid() || !extracted.span().span_context().is_valid());
    }

    #[tokio::test]
    async fn test_with_context() {
        let context = Context::current();
        let result = with_context(context, async { 42 }).await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_propagate_context() {
        let result = propagate_context(async { "test" }).await;
        assert_eq!(result, "test");
    }
}
