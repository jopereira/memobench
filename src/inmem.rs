use crate::generator::RawMemo;
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::warn;
use optd_original::cascades::{ExprId, GroupId, Memo, NaiveMemo};
use optd_original::nodes::{PlanNode, ArcPlanNode, NodeType, PlanNodeOrGroup, PredNode, Value};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::inmem::BenchPredTyp::Data;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BenchRelNodeTyp {
    Scan,
    Filter,
    Join,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BenchPredTyp {
    Data
}

impl NodeType for BenchRelNodeTyp {
    fn is_logical(&self) -> bool {
        match self {
            BenchRelNodeTyp::Scan => true,
            BenchRelNodeTyp::Filter => true,
            BenchRelNodeTyp::Join => true,
        }
    }

    type PredType = BenchPredTyp;
}

impl Display for BenchRelNodeTyp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                BenchRelNodeTyp::Scan => "Scan",
                BenchRelNodeTyp::Filter => "Filter",
                BenchRelNodeTyp::Join => "Join",
            }
        )
    }
}

impl Display for BenchPredTyp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "()")
    }
}

pub struct InMem {
    memo: NaiveMemo<BenchRelNodeTyp>,
    group_ids: Vec<GroupId>, // because get_all_group_ids() is pub(crate)
    entry: usize,
}

impl InMem {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        Ok(InMem {
            memo: NaiveMemo::new(Arc::new([])),
            group_ids: vec![],
            entry: 0,
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
                    children.push(PlanNodeOrGroup::Group(self.group_ids[*c]));
                }

                // build expressions with unique predicates
                let expr = ArcPlanNode::new(match e.op {
                    0 => PlanNode {
                        typ: BenchRelNodeTyp::Scan,
                        children: children,
                        predicates: vec![ Arc::new(PredNode{
                            typ: Data,
                            children: vec![],
                            data: Some(Value::UInt64(*j as u64)),
                        }) ],
                    },
                    1 => PlanNode {
                        typ: BenchRelNodeTyp::Filter,
                        children: children,
                        predicates: vec![ Arc::new(PredNode{
                            typ: Data,
                            children: vec![],
                            data: Some(Value::UInt64(*j as u64)),
                        }) ],
                    },
                    2 => PlanNode {
                        typ: BenchRelNodeTyp::Join,
                        children: children,
                        predicates: vec![ Arc::new(PredNode{
                            typ: Data,
                            children: vec![],
                            data: Some(Value::UInt64(*j as u64)),
                        }) ],
                    },
                    _ => unreachable!(),
                });

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
                            .add_expr_to_group(PlanNodeOrGroup::PlanNode(expr), group_id.unwrap())
                            .unwrap()
                    }
                };
            }
            if g.id >= self.group_ids.len() {
                self.group_ids.push(group_id.unwrap());
            } else {
                self.group_ids[g.id] = group_id.unwrap();
            }

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }
        }

        self.entry = memo.entry;

        Ok(hist)
    }

    fn retrieve(&mut self, mut rng: ChaCha8Rng, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let mut _tot = 0;
        for _ in 0..1000 {
            let g = rng.gen_range(0..memo.groups.len());

            let start = Instant::now();

            let group_expressions = self.memo.get_all_exprs_in_group(self.group_ids[g]);

            // do something with it
            let mut ids = vec![];
            for e in group_expressions {
                let expr = self.memo.get_expr_memoed(e);
                let pred = self.memo.get_pred(expr.predicates[0]);
                if let Some(Value::UInt64(v)) = pred.data {
                    ids.push(v as usize);
                }
            }

            if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                warn!("histogram overflow")
            }

            ids.sort();
            assert!(ids == memo.groups[g].exprs)
        }

        Ok(hist)
    }

    fn match_rules(&mut self) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut info = MatchInfo {
            visited_exprs: Default::default(),
            visited_groups: Default::default(),
            hist: Histogram::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?,
            last: Instant::now(),
        };
        self.explore_group(&mut info, self.group_ids[self.entry]);

        Ok(info.hist)
    }
}

struct MatchInfo {
    visited_exprs: HashSet<ExprId>,
    visited_groups: HashSet<GroupId>,
    hist: Histogram<u64>,
    last: Instant,
}

impl InMem {
    fn optimize_expression(&mut self, info: &mut MatchInfo, expr_id: ExprId) {
        if info.visited_exprs.insert(expr_id) {
            let top_expr = self.memo.get_expr_memoed(expr_id);

            // explore children first
            for c in top_expr.children.iter() {
                self.explore_group(info, *c);
            }

            // top_matches in optimize_expression task
            let mut _picks = vec![];
            if let BenchRelNodeTyp::Filter = top_expr.typ {

                // match_and_pick_expr in apply_rule task
                for bot_id in self
                    .memo
                    .get_all_exprs_in_group(top_expr.children[0])
                    .iter()
                {
                    let bot_expr = self.memo.get_expr_memoed(*bot_id);
                    if let BenchRelNodeTyp::Join = bot_expr.typ {
                        _picks.push(bot_expr.children.clone());

                        let now = Instant::now();
                        if let Err(_) = info
                            .hist
                            .record(now.duration_since(info.last).as_nanos() as u64)
                        {
                            warn!("histogram overflow")
                        }
                        info.last = now;
                    }
                }
            }
        }
    }

    fn explore_group(&mut self, info: &mut MatchInfo, group_id: GroupId) {
        if info.visited_groups.insert(group_id) {
            let exprs = self.memo.get_all_exprs_in_group(group_id);
            for expr in exprs {
                self.optimize_expression(info, expr);
            }
        }
    }
}
