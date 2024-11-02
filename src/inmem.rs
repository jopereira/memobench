use crate::generator::RawMemo;
use crate::inmem::BenchRelNodeTyp::{Filter, Join, Placeholder, Scan};
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::warn;
use optd_core::cascades::{GroupId, Memo};
use optd_core::rel_node::{RelNode, RelNodeRef, RelNodeTyp, Value};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BenchRelNodeTyp {
    Placeholder(GroupId),
    Scan,
    Filter,
    Join,
    List,
}

impl RelNodeTyp for BenchRelNodeTyp {
    fn is_logical(&self) -> bool {
        match self {
            BenchRelNodeTyp::Scan => true,
            BenchRelNodeTyp::Filter => true,
            BenchRelNodeTyp::Join => true,
            _ => false,
        }
    }

    fn group_typ(group_id: GroupId) -> Self {
        Self::Placeholder(group_id)
    }

    fn list_typ() -> Self {
        Self::List
    }

    fn extract_group(&self) -> Option<GroupId> {
        if let Self::Placeholder(group_id) = self {
            Some(*group_id)
        } else {
            None
        }
    }
}

impl Display for BenchRelNodeTyp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Placeholder(_) => "Placeholder",
                Scan => "Scan",
                Filter => "Filter",
                Join => "Join",
                BenchRelNodeTyp::List => "List",
            }
        )
    }
}

pub struct InMem {
    memo: Memo<BenchRelNodeTyp>,
    group_ids: Vec<GroupId>, // because get_all_group_ids() is pub(crate)
}

impl InMem {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(InMem {
            memo: Memo::new(Arc::new([])),
            group_ids: vec![],
        })
    }
}

impl Benchmark for InMem {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        for g in memo.groups.iter() {
            let start = Instant::now();
            let mut group_id = None;

            for j in g.exprs.iter() {
                let e = &memo.exprs[*j];

                let mut children = vec![];
                for c in e.children.iter() {
                    children.push(RelNodeRef::new(RelNode {
                        typ: Placeholder(self.group_ids[*c]),
                        children: vec![],
                        data: Some(Value::UInt64(*j as u64)),
                    }));
                }

                let expr = match e.children.len() {
                    0 => RelNodeRef::new(RelNode {
                        typ: Scan,
                        children: children,
                        data: Some(Value::UInt64(*j as u64)),
                    }),
                    1 => RelNodeRef::new(RelNode {
                        typ: Filter,
                        children: children,
                        data: Some(Value::UInt64(*j as u64)),
                    }),
                    2 => RelNodeRef::new(RelNode {
                        typ: Join,
                        children: children,
                        data: Some(Value::UInt64(*j as u64)),
                    }),
                    _ => unreachable!(),
                };

                match group_id {
                    None => {
                        // first expression in group, create group
                        let (g, e) = self.memo.add_new_expr(expr);
                        group_id = Some(g);
                        e
                    }
                    Some(_) => {
                        // add expression to existing group
                        self.memo
                            .add_expr_to_group(expr, group_id.unwrap())
                            .unwrap()
                    }
                };
            }
            self.group_ids.push(group_id.unwrap());

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }
        }

        Ok(hist)
    }

    fn retrieve(&mut self, mut rng: ChaCha8Rng) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let mut _tot = 0;
        for _ in 0..1000 {
            let g = rng.gen_range(0..self.group_ids.len());

            let start = Instant::now();

            let group_expressions = self.memo.get_all_exprs_in_group(self.group_ids[g]);

            // do something with it
            _tot += group_expressions.len();

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }
        }

        Ok(hist)
    }
}
