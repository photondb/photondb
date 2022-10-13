//! A storage engine for modern hardware.

#[cfg(test)]
mod tests {
    use photondb_engine::tree::{Error, Map, Options};

    #[test]
    fn main() -> Result<(), Error> {
        let map = Map::open(Options::default())?;
        map.put(b"hello", b"world")?;
        map.get(b"hello", |value| {
            assert_eq!(value.unwrap(), b"world");
        })?;
        map.delete(b"hello")?;
        map.get(b"hello", |value| assert_eq!(value, None))?;
        Ok(())
    }
}
