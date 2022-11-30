use std::fmt::Display;

use crate::util::atomic::Counter;

/// Statistics of a table.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    /// Statistics of succeed transactions.
    pub success: TxnStats,
    /// Statistics of conflicted transactions.
    pub conflict: TxnStats,
}

impl Stats {
    /// Sub other stats to produce an new stats.
    pub fn sub(&self, o: &Stats) -> Stats {
        Self {
            success: self.success.sub(&o.success),
            conflict: self.conflict.sub(&o.conflict),
        }
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TableStats_success: read: {}, write: {}, split_page: {}, reconcile_page: {}, consolidate_page: {}",
            self.success.read, self.success.write, self.success.split_page, self.success.reconcile_page, self.success.consolidate_page)?;
        writeln!(f, "TableStats_conflict: read: {}, write: {}, split_page: {}, reconcile_page: {}, consolidate_page: {}",
            self.conflict.read, self.conflict.write, self.conflict.split_page, self.conflict.reconcile_page, self.conflict.consolidate_page)
    }
}

#[derive(Default)]
pub(super) struct AtomicStats {
    pub(super) success: AtomicTxnStats,
    pub(super) conflict: AtomicTxnStats,
}

impl AtomicStats {
    pub(super) fn snapshot(&self) -> Stats {
        Stats {
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
}

#[derive(Default)]
pub(super) struct AtomicTxnStats {
    pub(super) read: Counter,
    pub(super) write: Counter,
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
            split_page: self.split_page.wrapping_sub(o.split_page),
            reconcile_page: self.reconcile_page.wrapping_sub(o.reconcile_page),
            consolidate_page: self.consolidate_page.wrapping_sub(o.consolidate_page),
            rewrite_page: self.rewrite_page.wrapping_sub(o.rewrite_page),
        }
    }
}
