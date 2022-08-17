# ![PhotonDB](docs/media/logo.png)

This is an experimental project to build a high performance data store in Rust.
The ultimate goal of this project is described in the [top-level design document](docs/design.md).

## Progress

Our first plan is to build a storage engine based on the Bw-Tree. Then build a standalone server with the storage engine.

The in-memory part of the storage engine is almost done at the moment. You can check the implementation in the `photondb-engine` crate.

## Benchmark

Some rough benchmarks on the in-memory storage engine so far:

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

You can run the bench tool with:

```
cargo build --release
target/release/bench --num-kvs 100000000 --num-ops 30000000 --num-threads 16 --get-weight 4 --put-weight 1
```

## References

- [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
- [Building a Bw-Tree Takes More Than Just Buzz Words](https://www.cs.cmu.edu/~huanche1/publications/open_bwtree.pdf)
- [Optimizing Bw-tree Indexing Performance](https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf)
- [LLAMA: A Cache/Storage Subsystem for Modern Hardware](http://www.vldb.org/pvldb/vol6/p877-levandoski.pdf)
- [Efficiently Reclaiming Space in a Log Structured Store](https://arxiv.org/abs/2005.00044)
- [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)
- [TinyLFU: A Highly Efficient Cache Admission Policy](https://arxiv.org/abs/1512.00727)