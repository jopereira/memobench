use crate::generator::RawMemo;
use crate::Benchmark;
use hdrhistogram::Histogram;
use log::{info, debug, warn};
use optd_persistent::entities::prelude::{CascadesGroup, LogicalExpression};
use optd_persistent::entities::*;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use sea_orm::{
    ActiveModelTrait, ActiveValue, Database, DatabaseConnection, EntityTrait, ModelTrait,
    PaginatorTrait, TransactionTrait,
};
use serde_json::json;
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

pub struct ExperimentalORM {
    runtime: Runtime,
    database: DatabaseConnection,
}

impl ExperimentalORM {
    pub fn new(url: String) -> Result<Self, Box<dyn Error>> {
        let runtime = Runtime::new().unwrap();
        let database = runtime.block_on(Database::connect(&url))?;

        info!("connected to database \"{}\"", url);

        Ok(ExperimentalORM { runtime, database })
    }
}

impl Benchmark for ExperimentalORM {
    fn add(&mut self, memo: &RawMemo) -> Result<Histogram<u64>, Box<dyn Error>> {
        self.runtime.block_on(async {
            let mut hist =
                Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

            // cleanup
            cascades_group::Entity::delete_many()
                .exec(&self.database)
                .await?;

            for (i, g) in memo.groups.iter().enumerate() {
                let start = Instant::now();

                let txn = self.database.begin().await?;

                debug!("loading group {}", i);

                let group = cascades_group::ActiveModel {
                    id: ActiveValue::Set(i as i32),
                    latest_winner: ActiveValue::Set(None),
                    in_progress: ActiveValue::Set(false),
                    is_optimized: ActiveValue::Set(false),
                    ..Default::default()
                }
                .insert(&txn)
                .await?;

                // FIXME: operator name from arity
                let ops = vec!["Scan", "Filter", "Join"];

                for e in g.exprs.iter() {
                    debug!("loading expression {}", e);

                    let l_expr = logical_expression::ActiveModel {
                        id: ActiveValue::set(*e as i32),
                        fingerprint: ActiveValue::Set(42), // Example fingerprint
                        data: ActiveValue::Set(json!({ // Example operator
                            "type": ops[memo.exprs[*e].children.len()],
                            "children": memo.exprs[*e].children,
                            "moredata": "...",
                        })),
                        group_id: ActiveValue::set(group.id.clone()),
                        ..Default::default()
                    }
                    .insert(&txn)
                    .await?;

                    debug!("loading junction {}<->{}", group.id, l_expr.id);

                    let _link = logical_group_junction::ActiveModel {
                        group_id: ActiveValue::set(group.id.clone()),
                        logical_expression_id: ActiveValue::set(l_expr.id.clone()),
                    }
                    .insert(&txn)
                    .await?;
                }

                debug!("committing transaction");

                txn.commit().await?;

                if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                    warn!("histogram overflow")
                }
            }

            Ok(hist)
        })
    }

    fn retrieve(&mut self, mut rng: ChaCha8Rng) -> Result<Histogram<u64>, Box<dyn Error>> {
        self.runtime.block_on(async {
            let mut hist =
                Histogram::<u64>::new_with_bounds(1, Duration::from_secs(1).as_nanos() as u64, 2)?;

            let n = CascadesGroup::find().count(&self.database).await? as i32;

            let mut _tot = 0;
            for _ in 0..1000 {
                let g = rng.gen_range(0..n);

                let start = Instant::now();

                debug!("retrieving group {}", g);

                let group = CascadesGroup::find_by_id(g)
                    .one(&self.database)
                    .await?
                    .unwrap();
                let group_expressions: Vec<logical_expression::Model> = group
                    .find_related(LogicalExpression)
                    .all(&self.database)
                    .await?;

                // do something with it
                _tot += group_expressions.len();

                if let Err(_) = hist.record(start.elapsed().as_nanos() as u64) {
                    warn!("histogram overflow")
                }
            }

            Ok(hist)
        })
    }
}
