# PhotonDB

This is an experimental project to explore how to build a high performance data store in Rust.

The current plan is to build an async runtime based on io_uring and a storage engine based on Bw-Tree first, and then build a standalone server on top of them.