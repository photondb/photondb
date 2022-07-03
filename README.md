# PhotonDB

This is an experimental project to explore how to build a high performance data store in Rust.

The first plan is to build an async runtime based on io_uring and a storage engine based on Bw-Tree. And then build a standalone server based on the runtime and the storage engine.

You can check the [top-level design document](docs/design.md) for more details about the project.

## References

- [The Bw-Tree: A B-tree for New Hardware Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)
- [Building a Bw-Tree Takes More Than Just Buzz Words](https://www.cs.cmu.edu/~huanche1/publications/open_bwtree.pdf)
- [Optimizing Bw-tree Indexing Performance](https://cseweb.ucsd.edu//~csjgwang/pubs/ICDE17_BwTree.pdf)
- [LLAMA: A Cache/Storage Subsystem for Modern Hardware](http://www.vldb.org/pvldb/vol6/p877-levandoski.pdf)
- [Efficiently Reclaiming Space in a Log Structured Store](https://arxiv.org/abs/2005.00044)
- [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)
- [TinyLFU: A Highly Efficient Cache Admission Policy](https://arxiv.org/abs/1512.00727)