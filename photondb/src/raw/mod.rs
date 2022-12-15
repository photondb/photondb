//! Raw PhotonDB APIs that can can run with different environments.

mod table;
pub use table::{Guard, Pages, Table, TableStats};

#[cfg(test)]
mod tree_test {
    use ::std::{collections::BTreeMap, panic, path::Path};
    use log::info;
    use quickcheck::*;
    use rand_distr::*;

    use crate::{raw::table, *};

    #[test]
    #[ignore]
    fn btreemap_cmp_test() {
        env_logger::init();
        let f = |ops| {
            let path = tempdir::TempDir::new("sdd").unwrap();
            let r = match panic::catch_unwind(|| {
                if let Err(e) = prop_cmp_btreemap(ops, path.path()) {
                    eprintln!("check fail: {:?}", e);
                    false
                } else {
                    true
                }
            }) {
                Ok(r) => r,
                Err(err) => panic!("{err:?}"),
            };
            info!("test finish and cleanup file");
            let _ = path.close();
            r
        };
        QuickCheck::new()
            .gen(Gen::new(2000))
            .tests(200)
            .max_tests(100 * 20)
            .quickcheck(f as fn(Vec<Op>) -> bool);
    }

    #[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
    struct Key(Vec<u8>);

    impl Arbitrary for Key {
        fn arbitrary(g: &mut Gen) -> Self {
            if Arbitrary::arbitrary(g) {
                let gs = g.size();
                let gamma = Gamma::new(0.3, gs as f64).unwrap();
                let v = gamma.sample(&mut rand::thread_rng());
                let len = if v > 3000.0 {
                    10000
                } else {
                    (v % 300.) as usize + 1
                };
                let space = g.choose(&(0..gs).collect::<Vec<_>>()).unwrap() + 1;
                let inner = (0..len)
                    .map(|_| *g.choose(&(0..space).collect::<Vec<_>>()).unwrap() as u8)
                    .collect();
                Self(inner)
            } else {
                let range = (0..2).collect::<Vec<_>>();
                let len = g.choose(&range).unwrap();
                let mut inner = vec![];
                for _ in 0..*len + 1 {
                    inner.push(Arbitrary::arbitrary(g));
                }
                Self(inner)
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new(
                self.0
                    .len()
                    .shrink()
                    .zip(::std::iter::repeat(self.0.clone()))
                    .map(|(len, underlying)| Self(underlying[..len].to_vec())),
            )
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        Set(Key, u8),
        Get(Key),
        Del(Key),
        #[allow(dead_code)]
        Restart,
    }

    impl Arbitrary for Op {
        fn arbitrary(g: &mut Gen) -> Self {
            let n: u8 = Arbitrary::arbitrary(g);
            if n % 10 == 1 {
                // TODO: uncomment this after recovery WAL done.
                // return Op::Restart;
            }
            let op = *g.choose(&(1..=3).collect::<Vec<_>>()).unwrap();
            match op {
                1 => Op::Set(Key::arbitrary(g), Arbitrary::arbitrary(g)),
                2 => Op::Get(Key::arbitrary(g)),
                3 => Op::Del(Key::arbitrary(g)),
                _ => unimplemented!(),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            match *self {
                Op::Set(ref k, v) => Box::new(k.shrink().map(move |sk| Op::Set(sk, v))),
                Op::Get(ref k) => Box::new(k.shrink().map(Op::Get)),
                Op::Del(ref k) => Box::new(k.shrink().map(Op::Del)),
                Op::Restart => Box::new(vec![].into_iter()),
            }
        }
    }

    fn prop_cmp_btreemap(ops: Vec<Op>, path: impl AsRef<Path>) -> Result<()> {
        let mut table_opt = TableOptions::default();
        table_opt.page_store.write_buffer_capacity = 16 << 10;
        table_opt.page_store.cache_strict_capacity_limit = true;
        table_opt.page_store.cache_capacity = 8 << 10;
        let mut table = std::Table::open(path.as_ref(), table_opt)?;
        info!("open table");
        let mut treemap: BTreeMap<Key, u16> = BTreeMap::new();

        let mut lsn = 0;
        for op in ops {
            match op {
                Op::Set(k, v) => {
                    let prev_tab = table.get(&k.0, lsn);
                    if let Err(Error::MemoryLimit) = prev_tab {
                        continue;
                    }
                    let prev_tab = prev_tab.unwrap();
                    lsn += 1;
                    info!("put {:?}", k);
                    table.put(&k.0, lsn, &[0, v]).unwrap();
                    lsn += 1;
                    let prev_map = treemap.insert(k.clone(), u16::from(v));
                    assert_eq!(
                        prev_tab.map(|v| bytes_to_u16(&v)),
                        prev_map,
                        "when setting key {:?}, expected old returned value to be {:?}\n{:?}",
                        k.0,
                        prev_map,
                        table,
                    );
                }
                Op::Get(k) => {
                    let res1 = table.get(&k.0, lsn);
                    if let Err(Error::MemoryLimit) = res1 {
                        continue;
                    }
                    let res1 = res1.unwrap();
                    let res1 = res1.map(|v| bytes_to_u16(&v));
                    lsn += 1;
                    let res2 = treemap.get(&k).cloned();
                    assert_eq!(
                        res1, res2,
                        "when get key {:?}, expected old returned value to be {:?}\n{:?}",
                        k, res2, table,
                    );
                }
                Op::Del(k) => {
                    info!("delete {:?}", k);
                    table.delete(&k.0, lsn).unwrap();
                    lsn += 1;
                    treemap.remove(&k);
                }
                Op::Restart => {
                    table.close().unwrap();
                    table = std::Table::open(path.as_ref(), TableOptions::default())?;
                }
            }
        }

        table.close().unwrap();

        Ok(())
    }

    fn bytes_to_u16(v: &[u8]) -> u16 {
        assert_eq!(v.len(), 2);
        (u16::from(v[0]) << 8) + u16::from(v[1])
    }
}
