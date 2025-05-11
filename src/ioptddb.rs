use std::collections::HashSet;
use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};
use hdrhistogram::Histogram;
use log::{debug, warn};
use optd_db::cascades::expressions::{LogicalExpression, LogicalExpressionId};
use optd_db::cascades::groups::{RelationalGroupId, ScalarGroupId};
use optd_db::cascades::memo::Memoize;
use optd_db::operators::relational::logical::filter::Filter;
use optd_db::operators::relational::logical::join::Join;
use optd_db::operators::relational::logical::scan::Scan;
use optd_db::operators::scalar::constants::Constant;
use optd_db::operators::scalar::ScalarOperator;
use optd_db::storage::memo::SqliteMemo;
use optd_db::values::OptdValue;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use tokio::runtime::Runtime;
use crate::Benchmark;
use crate::generator::RawMemo;

pub struct BenchOptdDb {
    memo: SqliteMemo,
    group_ids: Vec<RelationalGroupId>,
    entry: RelationalGroupId,
}

impl BenchOptdDb {
    pub fn new(database: &str) -> Result<Self,Box<dyn Error>> {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            Ok(BenchOptdDb { memo: SqliteMemo::new(database).await?, group_ids: vec![], entry: RelationalGroupId(0) })
        })
    }
}

impl Benchmark for BenchOptdDb {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            for g in memo.groups.iter() {
                let start = Instant::now();
                let mut group_id = None;

                for j in g.exprs.iter() {
                    let e = &memo.exprs[*j];

                    let expr = match e.op {
                        0 => LogicalExpression::Scan(Scan {
                            table_name: OptdValue::String((*j).to_string()),
                            predicate: self.predicate_from_val(*j).await,
                        }),
                        1 => LogicalExpression::Filter(Filter {
                            predicate: self.predicate_from_val(*j).await,
                            child: self.group_ids[e.children[0]],
                        }),
                        2 => LogicalExpression::Join(Join {
                            join_type: OptdValue::Int64(0),
                            left: self.group_ids[e.children[0]],
                            right: self.group_ids[e.children[1]],
                            condition: self.predicate_from_val(*j).await,
                        }),
                        _ => unreachable!(),
                    };

                    match group_id {
                        None => {
                            // first expression in group, create group
                            let e = self.memo.add_logical_expr(&expr).await?;
                            group_id = Some(e);
                        }
                        Some(id) => {
                            // add expression to existing group
                            self.memo.add_logical_expr_to_group(&expr, id).await?;
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

            self.entry = self.group_ids[memo.entry];

            Ok(hist)
        })
    }

    fn retrieve(&mut self, mut rng: ChaCha8Rng, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut hist =
            Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let mut _tot = 0;
            for g in (0..memo.groups.len()).chain((0..1000).map(|_| { rng.gen_range(0..memo.groups.len()) })) {

                let start = Instant::now();

                let group_expressions = self.memo.get_all_logical_exprs_in_group(self.group_ids[g]).await?;

                // do something with it
                let mut ids = vec![];
                for (_,expr) in group_expressions {
                    match expr.deref() {
                        LogicalExpression::Scan(expr) => {
                            ids.push(self.val_from_predicate(expr.predicate).await);
                        }
                        LogicalExpression::Filter(expr) => {
                            ids.push(self.val_from_predicate(expr.predicate).await);
                        }
                        LogicalExpression::Join(expr) => {
                            ids.push(self.val_from_predicate(expr.condition).await);
                        }
                        _ => {}
                    }
                }

                if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                    warn!("histogram overflow")
                }

                ids.sort();
                assert_eq!(ids, memo.groups[g].exprs, "incorrect memo (do not use --shuffle merge?)")
            }

            Ok(hist)
        })
    }

    fn match_rules(&mut self) -> Result<Histogram<u64>, Box<dyn Error>> {
        let mut info = MatchInfo {
            visited_exprs: Default::default(),
            visited_groups: Default::default(),
            hist: Histogram::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?,
            last: Instant::now(),
        };

        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            self.explore_group(&mut info, self.entry).await;

            Ok(info.hist)
        })
    }
}

struct MatchInfo {
    visited_exprs: HashSet<LogicalExpressionId>,
    visited_groups: HashSet<RelationalGroupId>,
    hist: Histogram<u64>,
    last: Instant,
}

impl BenchOptdDb {
    async fn predicate_from_val(&self, value: usize) -> ScalarGroupId {
        self.memo.add_scalar_expr(
            &ScalarOperator::Constant(
                Constant::new(OptdValue::Int64(value as i64)))).await.unwrap()
    }

    async fn val_from_predicate(&self, sid: ScalarGroupId) -> usize {
        if let ScalarOperator::Constant(c) = &self.memo.get_all_scalar_exprs_in_group(sid).await.unwrap()[0].1.deref() {
            if let OptdValue::Int64(v) = c.value {
                v as usize
            } else {
                panic!("invalid value")
            }
        } else {
            panic!("invalid predicate");
        }
    }

    async fn explore_group(&mut self, info: &mut MatchInfo, group_id: RelationalGroupId) {
        if info.visited_groups.insert(group_id) {
            let exprs = self.memo.get_all_logical_exprs_in_group(group_id).await.unwrap();
            for (id,expr) in exprs {
                Box::pin(self.optimize_expression(info, id, expr)).await;
            }
        }
    }

    async fn optimize_expression(&mut self, info: &mut MatchInfo, top_id: LogicalExpressionId, top_expr: Arc<LogicalExpression>) {
        if info.visited_exprs.insert(top_id) {

            // explore children first
            match top_expr.deref() {
                LogicalExpression::Scan(_) => {}
                LogicalExpression::Filter(expr) => {
                    self.explore_group(info, expr.child).await
                }
                LogicalExpression::Join(expr) => {
                    self.explore_group(info, expr.left).await;
                    self.explore_group(info, expr.right).await;
                }
                _ => {}
            }

            // top_matches in optimize_expression task
            let mut picks = vec![];
            if let LogicalExpression::Filter(f_expr) = top_expr.deref() {

                // match_and_pick_expr in apply_rule task
                for (_,bot_expr) in self
                    .memo
                    .get_all_logical_exprs_in_group(f_expr.child)
                    .await.unwrap().iter()
                {
                    if let LogicalExpression::Join(j_expr) = &bot_expr.deref() {
                        picks.push(vec![(j_expr.left, j_expr.right)]);

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
            debug!("found matches {:?}", picks)
        }
    }
}