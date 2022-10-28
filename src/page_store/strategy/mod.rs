use std::collections::HashMap;

use super::FileInfo;

pub(crate) trait StrategyBuilder: Send + Sync {
    fn build(&self, now: u32) -> Box<dyn GcPickStrategy>;
}

/// An abstraction describes the strategy of page files gc.
pub(crate) trait GcPickStrategy: Send + Sync {
    /// Collect file info and compute gc score.
    fn collect(&mut self, file_info: &FileInfo);

    /// Return the most suitable files for GC under the strategy.
    fn apply(&mut self) -> Option<u32>;
}

#[derive(PartialEq, PartialOrd, Debug, Clone)]
struct FileScore {
    score: f64,
    effective_rate: f64,
    write_amplify: f64,
    file_id: u32,
}

pub(crate) struct MinDeclineRateStrategy {
    now: u32,
    used_low: usize,
    #[allow(unused)]
    used_high: usize,
    used: usize,

    scores: Vec<FileScore>,
}

impl MinDeclineRateStrategy {
    fn new(now: u32, low: usize, high: usize) -> Self {
        MinDeclineRateStrategy {
            now,
            used_low: low,
            used_high: high,
            used: 0,
            scores: Vec::default(),
        }
    }
}

impl GcPickStrategy for MinDeclineRateStrategy {
    fn collect(&mut self, file_info: &FileInfo) {
        let file_id = file_info.get_file_id();
        let score = decline_rate(file_info, self.now);
        let effective_rate = file_info.effective_rate();
        let write_amplify = write_amplification(file_info);
        assert!(!score.is_nan());
        assert!(!effective_rate.is_nan());
        self.used += file_info.file_size();
        self.scores.push(FileScore {
            file_id,
            effective_rate,
            write_amplify,
            score,
        });
    }

    fn apply(&mut self) -> Option<u32> {
        if self.used < self.used_low || self.scores.len() < 2 {
            return None;
        }

        self.scores.sort_by(|a, b| {
            a.partial_cmp(b)
                .unwrap_or_else(|| a.file_id.cmp(&b.file_id))
        });

        if let Some(file) = self.scores.pop() {
            // FIXME: magic numbers.
            if file.effective_rate < 0.9 && file.write_amplify < 2.5 {
                return Some(file.file_id);
            }
        }
        None
    }
}

pub(crate) struct MinDeclineRateStrategyBuilder {
    // The min number of bytes required to start GC.
    used_low: usize,
    // The max number of bytes required to control write amplification.
    used_high: usize,
}

impl MinDeclineRateStrategyBuilder {
    pub(crate) fn new(low: usize, high: usize) -> Self {
        MinDeclineRateStrategyBuilder {
            used_low: low,
            used_high: high,
        }
    }
}

impl StrategyBuilder for MinDeclineRateStrategyBuilder {
    #[inline]
    fn build(&self, now: u32) -> Box<dyn GcPickStrategy> {
        Box::new(MinDeclineRateStrategy::new(
            now,
            self.used_low,
            self.used_high,
        ))
    }
}

pub(crate) fn decline_rate(file_info: &FileInfo, now: u32) -> f64 {
    let file_size = file_info.file_size();
    let free_size = file_size - file_info.effective_size();
    if free_size == 0 || file_info.up2() == now {
        return 0.0;
    }

    let file_size = file_size as f64;
    let free_size = free_size as f64;
    let num_active_pages = file_info.num_active_pages() as f64;
    let up2 = file_info.up2() as f64;
    let now = now as f64;

    // See "Efficiently Reclaiming Space in a Log Structured Store" section 5.1.3
    // "Transformed Declining Cost Equation" for details.
    ((file_size - free_size) / free_size).powi(2) / (num_active_pages * (now - up2))
}

#[allow(unused)]
pub(crate) fn total_write_amplification(file_infos: &HashMap<u32, FileInfo>) -> f64 {
    let empty_rate: f64 = file_infos.values().map(|i| i.empty_pages_rate()).sum();
    (1.0 / empty_rate) * (1.0 - empty_rate)
}

#[allow(unused)]
pub(crate) fn write_amplification(file_info: &FileInfo) -> f64 {
    let empty_rate = file_info.empty_pages_rate();

    // See "Efficiently Reclaiming Space in a Log Structured Store" section 2.1
    // "The Cost of Cleaning" for details.
    (1.0 / empty_rate) * (1.0 - empty_rate)
}
