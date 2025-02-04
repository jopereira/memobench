# memobench

Generates Cascades-style memo-like data for experimentation. Although the generated data is not really a valid relational expression, it should be good enough to test data structures and algorithms. It can also easily generate 1M "expressions" in under 1 second.

## Build

1. Clone memobench, [optd](https://github.com/cmu-db/optd), and [optd-experimental](https://github.com/cmu-db/optd-experimental) in the same directory.
2. cd memobench; cargo build --all-features

## Examples

Visualize a small memo:
```
cargo run -- -o memo.dot ; dot -T pdf memo.dot > memo.pdf ; open memo.pdf
```

Add 10M expressions to an in memory memo (from optd-original) and benchmark random retrieve:
```
cargo run --features=optd-orignal --release -- -g 1000000 -e 10 -a -r mem
```

Add a 1M expression DAG, triggering group merges, to an in memory memo (from optd-original) and benchmark rule matching:
```
cargo run --features=optd-orignal --release -- -g 1000 -d -e 10 -a -m  -u merge mem
```

Add a small DAG to Redis and benchmark retrieval and rule matching (the server needs to be running on localost):
```
cargo run --features=redis --release -- -g 1000 -d -e 10 -A  -u lookup redis
```

Benchmark the Redis memo with increasing number of groups, storing the result in a CSV file:
```
for i in 100 200 400 800 ; do \
    cargo run --features=redis --release -- -g $i -d -e 10 -a  -u lookup -c redis ; \
done > output.csv
```

## Options

The benchmark has sub-commands for each implementation of the memo data structure. Currently, mem (stored in memory, from optd-original), orm (stored in SQLite/Diesel, from optd), and redis (a simple implementation using Redis).

There are currently three stages: add (that generates data and populates the memo), retrieve, that does lookups on a populated memo (and implicitly checks that it has been correctly inserted), and match (that simulates matching a single rule against the current memo).

Data generation and the add stage have several options:

- Tree mode (default), generating a structure that looks like a relational expression inserted at the start, or DAG mode, generating a structure with multiple options that mimics the state of the memo after optimization is running for some time. Tree mode works best with -e1.
- Set target custom number of groups and average expressions per group. The final result might have slightly different number of expressions and groups due to randomness and to avoid dangling expressions.
- Use a custom seed to repeat a given run. This allows running the exact same data on multiple implementations and reproducible debugging.
- Shuffling the groups. By default, groups are inserted sequentially, which does not really exercise the memo. The lookup mode requires that the memo returns existing duplicate expressions, but does not trigger groups merges. The merge mode, makes sure that group merges (and recursive group merges) are needed. The latest is likely to be more useful as a torture test than as a benchmark.

There are some additional options for retrieving results:

- Dump the generated data to a GraphViz file.
- Print benchmarking results in CSV format to stdout.

See command-line help for syntax using --help for general options and --help on each subcommand for implementation specific options (e.g., database connection strings).

