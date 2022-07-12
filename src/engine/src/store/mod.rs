mod env;
use env::{RandomRead, SequentialRead, SequentialWrite};

mod error;
pub use error::{Error, Result};

mod footer;
