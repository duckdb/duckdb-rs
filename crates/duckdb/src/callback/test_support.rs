use std::{
    error::Error,
    fmt,
    panic::panic_any,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
pub(crate) struct PanickingDisplayError;

impl fmt::Display for PanickingDisplayError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("callback error display panic")
    }
}

impl Error for PanickingDisplayError {}

pub(crate) struct HazardousPanicPayload {
    _allocation: Box<[u8]>,
    drops: &'static AtomicU64,
}

impl HazardousPanicPayload {
    pub(crate) fn panic(drops: &'static AtomicU64) -> ! {
        panic_any(Self {
            _allocation: vec![0; 32].into_boxed_slice(),
            drops,
        })
    }
}

impl Drop for HazardousPanicPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("panic payload destructor panic")
    }
}
