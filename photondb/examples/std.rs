use photondb::{std::Table, Result, TableOptions};
use tempfile::tempdir;

fn main() -> Result<()> {
    let path = tempdir().unwrap();
    let table = Table::open(&path, TableOptions::default())?;
    let key = vec![1];
    let val1 = vec![2];
    let val2 = vec![3];
    // Simple CRUD operations.
    table.put(&key, 1, &val1)?;
    table.delete(&key, 2)?;
    table.put(&key, 3, &val2)?;
    assert_eq!(table.get(&key, 1)?, Some(val1.clone()));
    assert_eq!(table.get(&key, 2)?, None);
    assert_eq!(table.get(&key, 3)?, Some(val2.clone()));
    let guard = table.pin();
    // Get the value without copy.
    assert_eq!(guard.get(&key, 3)?, Some(val2.as_slice()));
    // Iterate the tree page by page.
    let mut pages = guard.pages();
    while let Some(page) = pages.next()? {
        for (k, v) in page {
            println!("{:?} {:?}", k, v);
        }
    }
    Ok(())
}
