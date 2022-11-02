# PhotonDB

[![crates][crates-badge]][crates-url]
[![docs][docs-badge]][docs-url]

[crates-badge]: https://img.shields.io/crates/v/photondb?style=flat-square
[crates-url]: https://crates.io/crates/photondb
[docs-badge]: https://img.shields.io/docsrs/photondb?style=flat-square
[docs-url]: https://docs.rs/photondb/latest/photondb

A high-performance storage engine for modern hardware and platforms.

PhotonDB is designed from scratch to leverage the power of modern multi-core chips, storage devices, operating systems, and programming languages.

Features:

- Latch-free data structures, scale to many cores.
- Log-structured persistent stores, optimized for flash storage.
- Asynchronous APIs and efficient file IO, powered by io_uring on Linux.

## Design

![Architecture](doc/media/architecture.drawio.svg)

## Progress

The in-memory part of the storage engine is almost done at the moment. We have published the `photondb-engine` crate v0.0.1.

Example:

```toml
[dependencies]
photondb-engine = "0.0.1"
```

```rust
use photondb_engine::tree::{Error, Map, Options};

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
```

## Benchmark

Some rough benchmarks on the in-memory storage engine with 100M keys:

| #threads |   get   |   put   | get:put = 4:1 |
|----------|---------|---------|---------------|
| 1        | 638468  | 521065  | 444558  |
| 2        | 1245346 | 1010584 | 938936  |
| 4        | 2371893 | 1897404 | 1768171 |
| 6        | 3317791 | 2622060 | 2489307 |
| 8        | 4099652 | 3240738 | 3102952 |
| 10       | 4738312 | 3681498 | 3418918 |
| 12       | 5281951 | 4148568 | 3801134 |
| 14       | 5826484 | 4636580 | 4309976 |
| 16       | 6364404 | 5114875 | 4812409 |

## References

- [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
- [Building a Bw-Tree Takes More Than Just Buzz Words](https://www.cs.cmu.edu/~huanche1/publications/open_bwtree.pdf)
- [LLAMA: A Cache/Storage Subsystem for Modern Hardware](http://www.vldb.org/pvldb/vol6/p877-levandoski.pdf)
- [Efficiently Reclaiming Space in a Log Structured Store](https://arxiv.org/abs/2005.00044)
- [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)
- [TinyLFU: A Highly Efficient Cache Admission Policy](https://arxiv.org/abs/1512.00727)