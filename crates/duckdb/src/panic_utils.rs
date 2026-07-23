use std::any::Any;

pub(crate) const NON_STRING_PANIC_PAYLOAD: &str = "non-string panic payload; use a string panic message for details";

#[cfg(test)]
pub(crate) fn panic_payload(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    {
        message.to_owned()
    } else {
        NON_STRING_PANIC_PAYLOAD.to_owned()
    }
}

#[cfg(any(feature = "vscalar", feature = "vtab"))]
pub(crate) fn downcast_panic_message(payload: Box<dyn Any + Send>) -> Result<String, Box<dyn Any + Send>> {
    payload
        .downcast::<String>()
        .map(|message| *message)
        .or_else(|payload| payload.downcast::<&'static str>().map(|message| (*message).to_owned()))
}
