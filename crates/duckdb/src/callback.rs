use std::{
    any::Any,
    error::Error,
    ffi::CString,
    fmt::Display,
    io::{self, Write},
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
};

use crate::panic_utils::panic_payload;

pub(crate) fn catch_callback<T, E>(callback: impl FnOnce() -> Result<T, E>) -> Result<T, String>
where
    E: Display,
{
    match catch_unwind(AssertUnwindSafe(|| callback().map_err(|error| error.to_string()))) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error),
        Err(payload) => Err(format!("Rust callback panicked: {}", take_panic_payload(payload))),
    }
}

pub(crate) fn catch_boxed_callback<T>(callback: impl FnOnce() -> Result<T, Box<dyn Error>>) -> Result<T, String> {
    catch_callback(|| callback().map_err(|error| format_error_chain(error.as_ref())))
}

pub(crate) fn catch_drop(callback: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(callback)) {
        let message = take_panic_payload(payload);
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _ = writeln!(
                io::stderr().lock(),
                "duckdb-rs caught a callback-state destructor panic: {}",
                message
            );
        }));
    }
}

fn take_panic_payload(payload: Box<dyn Any + Send>) -> String {
    let message = panic_payload(payload.as_ref());
    // A payload may panic again in Drop; leak it so containment cannot unwind across C.
    mem::forget(payload);
    message
}

pub(crate) fn error_c_string(error: &str) -> CString {
    let sanitized = error.replace('\0', "\\0");
    CString::new(sanitized).expect("NUL replacement must produce a valid C string")
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
        if depth == 16 && source.is_some() {
            message.push_str(": additional error sources omitted");
            break;
        }
    }
    message
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
    };

    use super::{catch_boxed_callback, catch_callback, catch_drop, error_c_string};

    struct PanickingDisplay;

    impl fmt::Display for PanickingDisplay {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("error display panic")
        }
    }

    impl fmt::Debug for PanickingDisplay {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            Ok(())
        }
    }

    impl Error for PanickingDisplay {}

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

    struct PanickingDropPayload;

    impl Drop for PanickingDropPayload {
        fn drop(&mut self) {
            panic!("panic payload destructor");
        }
    }

    #[test]
    fn catches_callback_and_error_display_panics() {
        let callback_error = catch_callback(|| -> Result<(), std::io::Error> { panic!("callback panic") }).unwrap_err();
        assert!(callback_error.contains("callback panic"));

        let display_error = catch_callback(|| -> Result<(), PanickingDisplay> { Err(PanickingDisplay) }).unwrap_err();
        assert!(display_error.contains("error display panic"));

        catch_drop(|| panic!("drop panic"));
    }

    #[test]
    fn preserves_error_sources_and_describes_non_string_panics() {
        let error =
            catch_boxed_callback(|| Err::<(), Box<dyn Error>>(Box::new(OuterError { source: RootCause }))).unwrap_err();
        assert_eq!(error, "outer error: root cause");

        let panic = catch_callback(|| -> Result<(), std::io::Error> { panic_any(7_u8) }).unwrap_err();
        assert!(panic.contains("non-string panic payload"));
        assert!(panic.contains("use a string panic message for details"));
    }

    #[test]
    fn callback_errors_are_valid_c_strings() {
        assert_eq!(error_c_string("before\0after").to_str().unwrap(), "before\\0after");
    }

    #[test]
    fn panicking_payload_destructors_are_contained() {
        let callback = catch_unwind(AssertUnwindSafe(|| {
            catch_callback(|| -> Result<(), std::io::Error> { panic_any(PanickingDropPayload) })
        }))
        .expect("caught callback payload must not panic while being dropped")
        .unwrap_err();
        assert!(callback.contains("Rust callback panicked"));

        catch_unwind(AssertUnwindSafe(|| {
            catch_drop(|| panic_any(PanickingDropPayload));
        }))
        .expect("caught destructor payload must not panic while being dropped");
    }
}
