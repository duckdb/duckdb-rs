use std::{
    any::{Any, type_name},
    error::Error,
    ffi::{CStr, CString, c_void},
    io::{self, Write},
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
};

use crate::panic_utils::{NON_STRING_PANIC_PAYLOAD, downcast_panic_message};

const MAX_ERROR_CAUSES: usize = 16;

/// Receives failures from a contained DuckDB callback.
pub(crate) trait CallbackErrorSink {
    fn set_c_error(&self, error: &CStr);

    /// Reports a Rust error message, escaping interior NUL bytes as `\0`.
    fn report_error(&self, error: &str) {
        self.set_c_error(&error_c_string(error));
    }
}

/// Runs a callback and reports any failure through DuckDB.
///
/// Callback panics and panics from error formatting or destruction are
/// contained here and never unwind to the caller. Failures are converted to a
/// DuckDB error before this function returns.
pub(crate) fn contain_callback(sink: &impl CallbackErrorSink, callback: impl FnOnce() -> Result<(), Box<dyn Error>>) {
    if let Err(error) = catch_boxed_callback(callback) {
        sink.report_error(&error);
    }
}

fn catch_boxed_callback(callback: impl FnOnce() -> Result<(), Box<dyn Error>>) -> Result<(), String> {
    match catch_unwind(AssertUnwindSafe(callback)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            let message = describe_error(error.as_ref());
            drop_or_report("error returned by callback", error);
            Err(message)
        }
        Err(payload) => Err(format!("Rust callback panicked: {}", take_panic_payload(payload))),
    }
}

fn describe_error(error: &dyn Error) -> String {
    catch_format(|| format_error_chain(error)).unwrap_or_else(|formatting_panic| {
        let debug_context = match catch_format(|| format!("{error:?}")) {
            Ok(message) => format!("callback error Debug context: {message}"),
            Err(debug_panic) => format!("callback error Debug formatting also panicked: {debug_panic}"),
        };
        format!("Rust callback error formatting panicked: {formatting_panic}; {debug_context}")
    })
}

/// Runs a formatting closure, converting a panic into its payload message.
fn catch_format(format: impl FnOnce() -> String) -> Result<String, String> {
    catch_unwind(AssertUnwindSafe(format)).map_err(take_panic_payload)
}

fn format_error_chain(error: &dyn Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    let mut depth = 0;
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
        depth += 1;
        if depth >= MAX_ERROR_CAUSES && source.is_some() {
            message.push_str(": additional error sources omitted");
            break;
        }
    }
    message
}

/// Frees a boxed callback state without allowing its destructor to unwind
/// across the C callback boundary.
///
/// # Safety
/// `ptr` must have been created by `Box::into_raw` for a `Box<T>` and must not
/// have been freed already.
pub(crate) unsafe extern "C" fn drop_boxed<T>(ptr: *mut c_void) {
    drop_or_report(type_name::<T>(), unsafe { Box::from_raw(ptr.cast::<T>()) });
}

/// Converts a Rust callback error into a C string.
///
/// Interior NUL bytes are escaped as the two-character sequence `\0`.
fn error_c_string(error: &str) -> CString {
    CString::new(error.replace('\0', "\\0")).expect("NUL replacement must produce a valid C string")
}

fn drop_or_report<T>(context: &str, value: T) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(value))) {
        let message = take_destructor_panic_payload(payload);
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _ = writeln!(
                io::stderr().lock(),
                "duckdb-rs caught a callback destructor panic for {context}: {message}"
            );
        }));
    }
}

fn take_panic_payload(payload: Box<dyn Any + Send>) -> String {
    take_payload_message(payload, |payload| drop_or_report("panic payload", payload))
}

/// Extracts a destructor panic payload's message.
///
/// This path must never dispose through `drop_or_report`: a hostile destructor
/// payload could then recurse without bound. Make one final drop attempt and
/// stop even if it produces another hostile payload.
fn take_destructor_panic_payload(payload: Box<dyn Any + Send>) -> String {
    take_payload_message(payload, drop_or_forget)
}

/// Extracts a panic payload's message, handing non-string payloads to
/// `dispose_unknown`.
fn take_payload_message(payload: Box<dyn Any + Send>, dispose_unknown: impl FnOnce(Box<dyn Any + Send>)) -> String {
    match downcast_panic_message(payload) {
        Ok(message) => message,
        Err(payload) => {
            dispose_unknown(payload);
            NON_STRING_PANIC_PAYLOAD.to_owned()
        }
    }
}

fn drop_or_forget<T>(value: T) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(value))) {
        // Reclaim known string payloads. An unknown payload may reproduce the
        // destructor panic, so it is deliberately forgotten (and leaked).
        if let Err(payload) = downcast_panic_message(payload) {
            mem::forget(payload);
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        error::Error,
        ffi::CStr,
        fmt,
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        CallbackErrorSink, MAX_ERROR_CAUSES, catch_boxed_callback, contain_callback, drop_boxed, error_c_string,
        format_error_chain,
    };

    static PANICKING_ERROR_DROPS: AtomicUsize = AtomicUsize::new(0);
    static RECURSIVE_PAYLOAD_DROPS: AtomicUsize = AtomicUsize::new(0);

    struct PanickingDisplay;

    #[derive(Default)]
    struct CapturingErrorSink(RefCell<Option<String>>);

    impl CallbackErrorSink for CapturingErrorSink {
        fn set_c_error(&self, error: &CStr) {
            self.0.replace(Some(error.to_string_lossy().into_owned()));
        }
    }

    impl fmt::Display for PanickingDisplay {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("error display panic")
        }
    }

    impl fmt::Debug for PanickingDisplay {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("PanickingDisplay with useful Debug context")
        }
    }

    impl Error for PanickingDisplay {}

    impl Drop for PanickingDisplay {
        fn drop(&mut self) {
            PANICKING_ERROR_DROPS.fetch_add(1, Ordering::SeqCst);
            panic!("error destructor panic")
        }
    }

    #[derive(Debug)]
    struct MessageWithPanickingDrop;

    impl fmt::Display for MessageWithPanickingDrop {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("callback error survives destructor panic")
        }
    }

    impl Error for MessageWithPanickingDrop {}

    impl Drop for MessageWithPanickingDrop {
        fn drop(&mut self) {
            panic!("error destructor panic after successful formatting")
        }
    }

    #[derive(Debug)]
    struct RootCause;

    impl fmt::Display for RootCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("root cause")
        }
    }

    impl Error for RootCause {}

    #[derive(Debug)]
    struct OuterError {
        source: RootCause,
    }

    impl fmt::Display for OuterError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("outer error")
        }
    }

    impl Error for OuterError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.source)
        }
    }

    #[derive(Debug)]
    struct PanickingSource;

    impl fmt::Display for PanickingSource {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("source display panic")
        }
    }

    impl Error for PanickingSource {}

    #[derive(Debug)]
    struct ErrorWithPanickingSource {
        source: PanickingSource,
    }

    impl fmt::Display for ErrorWithPanickingSource {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("useful outer error")
        }
    }

    impl Error for ErrorWithPanickingSource {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.source)
        }
    }

    #[derive(Debug)]
    struct ChainedError {
        source: Option<Box<ChainedError>>,
    }

    impl ChainedError {
        fn with_causes(causes: usize) -> Self {
            Self {
                source: (causes > 0).then(|| Box::new(Self::with_causes(causes - 1))),
            }
        }
    }

    impl fmt::Display for ChainedError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("chain error")
        }
    }

    impl Error for ChainedError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.source.as_deref().map(|source| source as &dyn Error)
        }
    }

    #[derive(Debug)]
    struct CyclicError;

    impl fmt::Display for CyclicError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("cyclic error")
        }
    }

    impl Error for CyclicError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self)
        }
    }

    struct PanickingDropPayload {
        _allocation: Box<[u8]>,
    }

    impl Drop for PanickingDropPayload {
        fn drop(&mut self) {
            panic!("panic payload destructor")
        }
    }

    // The final self-reproduced payload is deliberately forgotten, so this
    // sentinel must remain allocation-free for leak-sanitized test runs.
    struct RecursivePanicPayload;

    impl Drop for RecursivePanicPayload {
        fn drop(&mut self) {
            RECURSIVE_PAYLOAD_DROPS.fetch_add(1, Ordering::SeqCst);
            panic_any(Self)
        }
    }

    struct PanickingStateDrop {
        _allocation: Box<[u8]>,
    }

    impl Drop for PanickingStateDrop {
        fn drop(&mut self) {
            panic!("drop panic")
        }
    }

    struct HazardousStateDrop;

    impl Drop for HazardousStateDrop {
        fn drop(&mut self) {
            panic_any(PanickingDropPayload {
                _allocation: vec![0; 32].into_boxed_slice(),
            })
        }
    }

    #[test]
    fn catches_callback_and_error_display_panics() {
        let callback_error =
            catch_boxed_callback(|| -> Result<(), Box<dyn Error>> { panic!("callback panic") }).unwrap_err();
        assert!(callback_error.contains("callback panic"));

        PANICKING_ERROR_DROPS.store(0, Ordering::SeqCst);
        let display_error = catch_unwind(AssertUnwindSafe(|| {
            catch_boxed_callback(|| Err::<(), Box<dyn Error>>(Box::new(PanickingDisplay)))
        }))
        .expect("error formatting and destruction panics must both be contained")
        .unwrap_err();
        assert!(display_error.contains("callback error formatting panicked"));
        assert!(display_error.contains("error display panic"));
        assert!(display_error.contains("PanickingDisplay with useful Debug context"));
        assert_eq!(PANICKING_ERROR_DROPS.load(Ordering::SeqCst), 1);

        let message_error =
            catch_boxed_callback(|| Err::<(), Box<dyn Error>>(Box::new(MessageWithPanickingDrop))).unwrap_err();
        assert_eq!(message_error, "callback error survives destructor panic");

        let ptr = Box::into_raw(Box::new(PanickingStateDrop {
            _allocation: vec![0; 32].into_boxed_slice(),
        }))
        .cast();
        unsafe { drop_boxed::<PanickingStateDrop>(ptr) };
    }

    #[test]
    fn contained_callbacks_report_failures_through_the_sink() {
        let sink = CapturingErrorSink::default();
        contain_callback(&sink, || Err::<(), Box<dyn Error>>("callback before\0after".into()));

        assert_eq!(sink.0.take().as_deref(), Some("callback before\\0after"));
    }

    #[test]
    fn preserves_error_sources_and_describes_non_string_panics() {
        let error =
            catch_boxed_callback(|| Err::<(), Box<dyn Error>>(Box::new(OuterError { source: RootCause }))).unwrap_err();
        assert_eq!(error, "outer error: root cause");

        let error = catch_boxed_callback(|| {
            Err::<(), Box<dyn Error>>(Box::new(ErrorWithPanickingSource {
                source: PanickingSource,
            }))
        })
        .unwrap_err();
        assert!(error.contains("callback error formatting panicked"));
        assert!(error.contains("source display panic"));
        assert!(error.contains("ErrorWithPanickingSource { source: PanickingSource }"));

        let panic = catch_boxed_callback(|| -> Result<(), Box<dyn Error>> { panic_any(7_u8) }).unwrap_err();
        assert!(panic.contains("non-string panic payload"));
        assert!(panic.contains("use a string panic message for details"));
    }

    #[test]
    fn truncates_deep_and_cyclic_error_chains() {
        let deep = format_error_chain(&ChainedError::with_causes(MAX_ERROR_CAUSES + 4));
        assert_eq!(deep.matches("chain error").count(), MAX_ERROR_CAUSES + 1);
        assert!(deep.ends_with("additional error sources omitted"));

        let cyclic = format_error_chain(&CyclicError);
        assert_eq!(cyclic.matches("cyclic error").count(), MAX_ERROR_CAUSES + 1);
        assert!(cyclic.ends_with("additional error sources omitted"));
    }

    #[test]
    fn callback_errors_are_valid_c_strings() {
        assert_eq!(error_c_string("ordinary error").to_str().unwrap(), "ordinary error");
        assert_eq!(error_c_string("before\0after").to_str().unwrap(), "before\\0after");
    }

    #[test]
    fn panicking_payload_destructors_are_contained() {
        let callback = catch_unwind(AssertUnwindSafe(|| {
            catch_boxed_callback(|| -> Result<(), Box<dyn Error>> {
                panic_any(PanickingDropPayload {
                    _allocation: vec![0; 32].into_boxed_slice(),
                })
            })
        }))
        .expect("caught callback payload must not panic while being dropped")
        .unwrap_err();
        assert!(callback.contains("Rust callback panicked"));

        let ptr = Box::into_raw(Box::new(HazardousStateDrop)).cast();
        catch_unwind(AssertUnwindSafe(|| unsafe {
            drop_boxed::<HazardousStateDrop>(ptr);
        }))
        .expect("caught destructor payload must not panic while being dropped");
    }

    #[test]
    fn recursive_panic_payload_destruction_is_bounded() {
        RECURSIVE_PAYLOAD_DROPS.store(0, Ordering::SeqCst);
        let callback = catch_unwind(AssertUnwindSafe(|| {
            catch_boxed_callback(|| -> Result<(), Box<dyn Error>> { panic_any(RecursivePanicPayload) })
        }))
        .expect("recursive panic payload destruction must remain contained")
        .unwrap_err();

        assert!(callback.contains("Rust callback panicked"));
        assert_eq!(RECURSIVE_PAYLOAD_DROPS.load(Ordering::SeqCst), 2);
    }
}
