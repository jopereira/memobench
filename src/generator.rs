use log::info;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::cmp::max;
use std::collections::HashSet;
use std::io::Write;
use std::time::Instant;

#[derive(Clone)]
pub struct RawExpr {
    pub op: usize,
    pub children: Vec<usize>,
}

#[derive(Clone)]
pub struct RawGroup {
    pub exprs: Vec<usize>,
    pub id: usize,
}

pub struct RawMemo {
    pub exprs: Vec<RawExpr>,
    pub groups: Vec<RawGroup>,
    pub entry: usize,
}

impl RawMemo {
    pub fn new(ngroups: usize, nexprs: usize, dag: bool, mut rng: ChaCha8Rng) -> Self {
        // FIXME: MAGIC NUMBERS
        let weights = [10, 30, 30]; // distribution operator arity
        let proximity = 4; // proximity factor (1 for no proximity preference)

        info!("target: {} groups, {} expressions/group", ngroups, nexprs);

        let start = Instant::now();

        let mut memo = RawMemo {
            exprs: Vec::new(),
            groups: Vec::new(),
            entry: 0,
        };

        let mut tot = 0;
        let mut cnt = 0;
        for (i, v) in weights.iter().enumerate() {
            tot += i * v;
            cnt += v;
        }
        let dist = WeightedIndex::new(&weights).unwrap();

        // Generate groups
        let mut gqueue: Vec<usize> = vec![];
        while memo.groups.len() < ngroups || gqueue.len() > 1 {
            let mut exprs: Vec<usize> = vec![];

            let ngen = rng.gen_range(0..nexprs * 2);

            // Generate expressions even if no groups to reference (will be a scan!)
            while exprs.len() < ngen {
                let arity = dist.sample(&mut rng);
                let mut children: Vec<usize> = vec![];
                let mut cset: HashSet<usize> = HashSet::new();
                for i in 0..arity {
                    if gqueue.len() > 0 {
                        let idx = rng.gen_range(0..gqueue.len());
                        // avoid using the same group twice
                        // as an operand to the same expression
                        let c = gqueue[idx];
                        if !cset.contains(&c) {
                            cset.insert(c);
                            children.push(gqueue[idx]);
                            gqueue.remove(idx);
                        }
                    }
                    if children.len() <= i {
                        // failed to find a group, make one extra 'scan' node now
                        children.push(memo.groups.len());
                        memo.groups.push(RawGroup {
                            exprs: vec![memo.exprs.len()],
                            id: memo.groups.len(),
                        });
                        memo.exprs.push(RawExpr {
                            op: 0,
                            children: vec![],
                        });
                    }
                }
                let expr_id = memo.exprs.len();
                exprs.push(expr_id);

                let op = children.len(); // FIXME: more ops?
                memo.exprs.push(RawExpr { op, children });
            }

            if ngen > 0 {
                let group_id = memo.groups.len();
                memo.groups.push(RawGroup { exprs: exprs, id: group_id });

                // While we don't have enough groups, collect operands for future expressions
                if group_id < ngroups && dag {
                    // replenish groups to be referenced
                    let ng = match nexprs * tot / cnt {
                        d if d > 0 => rng.gen_range(0..d * 2),
                        _ => 0,
                    };

                    let m = max(0, (group_id as i32) - (ngroups as i32) / proximity) as usize;
                    for _ in 0..ng {
                        gqueue.push(rng.gen_range(m..group_id + 1));
                    }
                }

                // Add at least this group, so that it is referenced or the last
                gqueue.push(group_id);
            }
        }

        memo.entry = gqueue[0];

        info!(
            "result: {} groups, {} expressions ({:?})",
            memo.groups.len(),
            memo.exprs.len(),
            start.elapsed(),
        );

        memo
    }

    pub fn shuffle(&self, chunk: usize, merge: bool) -> RawMemo {
        assert!(chunk > 1);

        let mut groups = self.groups.clone();
        let mut i = 0;

        while i < groups.len() {
            let g = &groups[i];
            let gid = g.id;
            if g.exprs.len() > chunk {
                let upd = g.exprs[..chunk].to_vec();
                let rest = if merge {
                    // trigger merge 1,2,3,4,5,6,7 --> 1,2,3 + 4,5,6 + 7,1,4
                    let mut v = g.exprs[chunk..].to_vec();
                    v.push(g.exprs[0]);
                    v
                } else {
                    // don't trigger merge 1,2,3,4,5,6,7 --> 1,2,3 - 3,4,5, - 5,6,7
                    g.exprs[chunk-1..].to_vec()
                };
                groups[i].exprs = upd;
                groups.push(RawGroup{ exprs: rest, id: gid })
            }
            i = i+1;
        }

        RawMemo {
            exprs: self.exprs.clone(),
            groups: groups,
            entry: self.entry,
        }
    }

    pub fn dump(&self, writer: &mut Box<dyn Write>) -> std::io::Result<()> {
        writeln!(writer, "digraph Memo {{")?;
        writeln!(writer, "node [colorscheme=set312]")?;
        for (i, g) in self.groups.iter().enumerate() {
            writeln!(writer, "\"g{}\" [shape=box];", i)?;
            for e in g.exprs.iter() {
                writeln!(writer, "\"g{}\" -> \"e{}\";", i, e)?;
            }
        }
        for (i, e) in self.exprs.iter().enumerate() {
            writeln!(writer, "\"e{}\" [shape=oval,style=filled,color={}];", i, e.op+1)?;
            for c in e.children.iter() {
                writeln!(writer, "\"e{}\" -> \"g{}\";", i, c)?;
            }
        }
        writeln!(writer, "}}")
    }

    pub fn len(&self) -> usize {
        self.exprs.len()
    }
}