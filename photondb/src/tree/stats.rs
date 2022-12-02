use std::fmt::Display;

use crate::util::atomic::Counter;

/// Statistics of a tree.
#[derive(Clone, Debug, Default)]
pub struct TreeStats {
    /// Statistics of succeed transactions.
    pub success: TxnStats,
    /// Statistics of conflicted transactions.
    pub conflict: TxnStats,
}

impl TreeStats {
    /// Sub other stats to produce an new stats.
    pub fn sub(&self, o: &TreeStats) -> TreeStats {
        Self {
            success: self.success.sub(&o.success),
            conflict: self.conflict.sub(&o.conflict),
        }
    }
}

impl Display for TreeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TreeStats_success: read: {}, write: {}, split_page: {}, reconcile_page: {}, consolidate_page: {}, write_bytes: {}",
            self.success.read, self.success.write, self.success.split_page, self.success.reconcile_page, self.success.consolidate_page, self.success.write_bytes)?;
        writeln!(f, "TreeStats_conflict: read: {}, write: {}, split_page: {}, reconcile_page: {}, consolidate_page: {}",
            self.conflict.read, self.conflict.write, self.conflict.split_page, self.conflict.reconcile_page, self.conflict.consolidate_page)
    }
}

#[derive(Default)]
pub(super) struct AtomicStats {
    pub(super) success: AtomicTxnStats,
    pub(super) conflict: AtomicTxnStats,
}

impl AtomicStats {
    pub(super) fn snapshot(&self) -> TreeStats {
        TreeStats {
            success: self.success.snapshot(),
            conflict: self.conflict.snapshot(),
        }
    }
}

/// Statistics of tree transactions.
#[derive(Clone, Debug, Default)]
pub struct TxnStats {
    pub read: u64,
    pub write: u64,
    pub split_page: u64,
    pub reconcile_page: u64,
    pub consolidate_page: u64,
    pub rewrite_page: u64,
    pub write_bytes: u64,
}

#[derive(Default)]
pub(super) struct AtomicTxnStats {
    pub(super) read: Counter,
    pub(super) write: Counter,
    pub(super) write_bytes: Counter,
    pub(super) split_page: Counter,
    pub(super) reconcile_page: Counter,
    pub(super) consolidate_page: Counter,
    pub(super) rewrite_page: Counter,
}

impl AtomicTxnStats {
    pub(super) fn snapshot(&self) -> TxnStats {
        TxnStats {
            read: self.read.get(),
            write: self.write.get(),
            write_bytes: self.write_bytes.get(),
            split_page: self.split_page.get(),
            reconcile_page: self.reconcile_page.get(),
            consolidate_page: self.consolidate_page.get(),
            rewrite_page: self.rewrite_page.get(),
        }
    }
}

impl TxnStats {
    pub(super) fn sub(&self, o: &TxnStats) -> TxnStats {
        TxnStats {
            read: self.read.wrapping_sub(o.read),
            write: self.write.wrapping_sub(o.write),
            write_bytes: self.write_bytes.wrapping_sub(o.write_bytes),
            split_page: self.split_page.wrapping_sub(o.split_page),
            reconcile_page: self.reconcile_page.wrapping_sub(o.reconcile_page),
            consolidate_page: self.consolidate_page.wrapping_sub(o.consolidate_page),
            rewrite_page: self.rewrite_page.wrapping_sub(o.rewrite_page),
        }
    }
}
