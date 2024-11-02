# memobench

Generates Cascades-style memo-like data for experimentation. Although the generated data is not really a valid relational expression, it should be good enough to test data structures and algorithms. It can also easily generate 1M "expressions" in under 1 second.

## Build

1. Clone memobench, [optd](https://github.com/cmu-db/optd), and [optd-experimental](https://github.com/cmu-db/optd-experimental) in the same directory.
2. cd memobench; cargo build

## Run

The benchmark has sub-commands for each implementation of the memo data structure. Currently, null, in-mem (from optd) and exp-orm (from optd-experimental).

There are currently two stages: populate (that implicitly generates data) and retrieve, that runs on a populated data structure.

Remember to run migrate from optd-persistent to initialize the database before using orm-exp.

See command-line help for syntax.

## Options

The benchmark has several options:

- Tree mode (default) or DAG mode. Tree mode works best with -e1.
- Set target custom number of groups and average expressions per group. The final result might have slightly different number of expressions and groups due to randomness and to avoid dangling expressions.
- Dump the generated data to a GraphViz file.
- Use a custom seed to repeat a given run.

## Examples

Visualize a small memo:
```
cargo run -- -o memo.dot null ; dot -T pdf memo.dot > memo.pdf ; open memo.pdf
```

Load and retrieve 10M expressions:
```
cargo run --release -- -g 1000000 -e 10 -p -r in-mem
```
