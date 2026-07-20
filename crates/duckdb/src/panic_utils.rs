use std::any::Any;

#[cfg_attr(not(any(test, feature = "vscalar", feature = "vtab")), allow(dead_code))]
pub(crate) fn panic_payload(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    {
        message.to_owned()
    } else {
        "non-string panic payload; use a string panic message for details".to_owned()
    }
}
