use std::fmt::Display;

pub(crate) trait ResultExt<T> {
    fn or_panic(self) -> T;
}

impl<T, E: Display> ResultExt<T> for std::result::Result<T, E> {
    fn or_panic(self) -> T {
        self.unwrap_or_else(|err| panic!("{err}"))
    }
}
