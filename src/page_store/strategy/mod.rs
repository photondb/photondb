use super::FileInfo;

#[allow(unused)]
pub(crate) fn decline_rate(file_info: &FileInfo, now: u32) -> f64 {
    let file_meta = file_info.meta();
    let file_size = file_meta.file_size();
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
