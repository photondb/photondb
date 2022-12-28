use std::collections::HashMap;

use rustc_hash::FxHashMap;

use super::{FileInfo, PageGroup};

pub(crate) trait StrategyBuilder: Send + Sync {
    fn build(&self, now: u32) -> Box<dyn ReclaimPickStrategy>;
}

/// An abstraction describes the strategy of page files reclaiming.
pub(crate) trait ReclaimPickStrategy: Send + Sync {
    /// Collect map file info and compute reclamation score.
    fn collect_file(&mut self, page_groups: &FxHashMap<u32, PageGroup>, file_info: &FileInfo);

    /// Return the most suitable files for reclaiming under the strategy.
    fn apply(&mut self) -> Option<(u32, usize /* active size */)>;
}

pub(crate) struct MinDeclineRateStrategy {
    now: u32,

    sorted: bool,
    scores: Vec<FileScore>,
}

pub(crate) struct MinDeclineRateStrategyBuilder;

#[derive(PartialEq, PartialOrd, Debug, Clone)]
struct FileScore {
    score: f64,
    effective_rate: f64,
    write_amplify: f64,
    active_size: usize,
    file_id: u32,
}

#[derive(Debug)]
struct FileSummary {
    num_active_pages: usize,
    total_page_size: usize,
    effective_size: usize,
    effective_rate: f64,
    empty_pages_rate: f64,
    up2: u32,
}

impl MinDeclineRateStrategy {
    fn new(now: u32) -> Self {
        MinDeclineRateStrategy {
            now,
            sorted: false,
            scores: Vec::default(),
        }
    }

    fn collect(&mut self, file_id: u32, summary: &FileSummary) {
        let score = decline_rate(summary, self.now);
        let effective_rate = summary.effective_rate;
        let write_amplify = write_amplification(summary.empty_pages_rate);
        assert!(!score.is_nan());
        assert!(!effective_rate.is_nan());
        assert!(!effective_rate.is_infinite());
        self.scores.push(FileScore {
            file_id,
            effective_rate,
            write_amplify,
            active_size: summary.effective_size,
            score,
        });
    }
}

impl ReclaimPickStrategy for MinDeclineRateStrategy {
    fn collect_file(&mut self, page_groups: &FxHashMap<u32, PageGroup>, file_info: &FileInfo) {
        let file_id = file_info.meta().file_id;
        let summary = FileSummary::from((page_groups, file_info));
        self.collect(file_id, &summary);
    }

    fn apply(&mut self) -> Option<(u32, usize)> {
        if !self.sorted {
            self.sorted = true;
            self.scores.sort_unstable_by(|a, b| {
                a.partial_cmp(b)
                    .unwrap_or_else(|| a.file_id.cmp(&b.file_id))
            });
        }

        if self.scores.len() < 2 {
            return None;
        }

        self.scores.pop().map(|f| (f.file_id, f.active_size))
    }
}

impl StrategyBuilder for MinDeclineRateStrategyBuilder {
    #[inline]
    fn build(&self, now: u32) -> Box<dyn ReclaimPickStrategy> {
        Box::new(MinDeclineRateStrategy::new(now))
    }
}

impl From<(&FxHashMap<u32, PageGroup>, &FileInfo)> for FileSummary {
    fn from((file_infos, info): (&FxHashMap<u32, PageGroup>, &FileInfo)) -> Self {
        let meta = info.meta();
        let up2 = info.up2();
        let page_groups = &meta.page_groups;
        let mut num_active_pages = 0;
        let mut effective_size = 0;
        let mut total_pages = 0;
        let mut total_page_size = 0;
        for page_group in page_groups.keys() {
            let page_group = file_infos
                .get(page_group)
                .expect("Virtual page file must exists");
            num_active_pages += page_group.num_active_pages();
            effective_size += page_group.effective_size();
            total_pages += page_group.meta().total_pages();
            total_page_size += page_group.meta().total_page_size();
        }
        let effective_rate = effective_size as f64 / (total_page_size as f64);
        let empty_pages_rate = if total_pages > 0 {
            1.0 - (num_active_pages as f64 / total_pages as f64)
        } else {
            0.0
        };
        FileSummary {
            num_active_pages,
            effective_size,
            effective_rate,
            empty_pages_rate,
            total_page_size,
            up2,
        }
    }
}

fn decline_rate(summary: &FileSummary, now: u32) -> f64 {
    let num_active_pages = summary.num_active_pages;
    if num_active_pages == 0 {
        return 0.0;
    }

    let total_page_size = summary.total_page_size;
    let effective_size = summary.effective_size;
    let free_size = total_page_size.saturating_sub(effective_size);
    if free_size == 0 || summary.up2 == now {
        return f64::MIN;
    }

    let num_active_pages = num_active_pages as f64;
    let effective_size = effective_size as f64;
    let free_size = free_size as f64;
    let up2 = summary.up2 as f64;
    let now = now as f64;

    // See "Efficiently Reclaiming Space in a Log Structured Store" section 5.1.3
    // "Transformed Declining Cost Equation" for details.
    -(effective_size / free_size).powi(2) / (num_active_pages * (now - up2))
}

#[allow(unused)]
pub(crate) fn total_write_amplification(file_infos: &HashMap<u32, PageGroup>) -> f64 {
    let empty_rate: f64 = file_infos
        .values()
        .filter(|i| !i.is_empty())
        .map(|i| i.empty_pages_rate())
        .sum();
    (1.0 / empty_rate) * (1.0 - empty_rate)
}

#[inline]
pub(crate) fn write_amplification(empty_rate: f64) -> f64 {
    // See "Efficiently Reclaiming Space in a Log Structured Store" section 2.1
    // "The Cost of Cleaning" for details.
    (1.0 / empty_rate) * (1.0 - empty_rate)
}
