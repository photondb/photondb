use std::collections::HashMap;

use super::{page_file::FileId, FileInfo, MapFileInfo};

pub(crate) trait StrategyBuilder: Send + Sync {
    fn build(&self, now: u32) -> Box<dyn ReclaimPickStrategy>;
}

/// An abstraction describes the strategy of page files reclaiming.
pub(crate) trait ReclaimPickStrategy: Send + Sync {
    /// Collect file info and compute reclamation score.
    fn collect_page_file(&mut self, file_info: &FileInfo);

    /// Collect map file info and compute reclamation score.
    fn collect_map_file(&mut self, virtual_infos: &HashMap<u32, FileInfo>, file_info: &MapFileInfo);

    /// Return the most suitable files for reclaiming under the strategy.
    fn apply(&mut self) -> Option<(FileId, usize /* active size */)>;
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
    file_id: FileId,
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

    fn collect(&mut self, file_id: FileId, summary: &FileSummary) {
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
    fn collect_page_file(&mut self, file_info: &FileInfo) {
        let file_id = file_info.get_file_id();
        let summary = FileSummary::from(file_info);
        self.collect(FileId::Page(file_id), &summary);
    }

    fn collect_map_file(
        &mut self,
        virtual_infos: &HashMap<u32, FileInfo>,
        file_info: &MapFileInfo,
    ) {
        let file_id = file_info.file_id();
        let summary = FileSummary::from((virtual_infos, file_info));
        self.collect(FileId::Map(file_id), &summary);
    }

    fn apply(&mut self) -> Option<(FileId, usize)> {
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

impl From<&FileInfo> for FileSummary {
    fn from(info: &FileInfo) -> Self {
        FileSummary {
            num_active_pages: info.num_active_pages(),
            total_page_size: info.total_page_size(),
            effective_size: info.effective_size(),
            effective_rate: info.effective_rate(),
            empty_pages_rate: info.empty_pages_rate(),
            up2: info.up2(),
        }
    }
}

impl From<(&HashMap<u32, FileInfo>, &MapFileInfo)> for FileSummary {
    fn from((file_infos, info): (&HashMap<u32, FileInfo>, &MapFileInfo)) -> Self {
        let meta = info.meta();
        let up2 = info.up2();
        let page_files = meta.page_files();
        let mut num_active_pages = 0;
        let mut effective_size = 0;
        let mut total_pages = 0;
        let mut total_page_size = 0;
        for page_file in page_files.keys() {
            let partial_info = file_infos
                .get(page_file)
                .expect("Virtual page file must exists");
            num_active_pages += partial_info.num_active_pages();
            effective_size += partial_info.effective_size();
            total_pages += partial_info.total_pages();
            total_page_size += partial_info.total_page_size();
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
pub(crate) fn total_write_amplification(file_infos: &HashMap<u32, FileInfo>) -> f64 {
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
