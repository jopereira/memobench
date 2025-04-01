# memobench

Generates Cascades-style memo-like data for experimentation. Although the generated data is not really a valid relational expression, it should be good enough to test data structures and algorithms. It can also easily generate 1M "expressions" in under 1 second.

## Build

To build the main executable with all features:
```
git clone https://github.com/jopereira/memobench.git
cd memobench
cargo build --all-features
```

To run the Calcite implementation build the Java bridge with:
```
cd bridge
mvn package
```

## Examples

Visualize a small memo:
```
cargo run -- -o memo.dot ; dot -T pdf memo.dot > memo.pdf ; open memo.pdf
```

Add 10M expressions to an in-memory memo (from optd-original) and benchmark random retrieve:
```
cargo run --features=optd-orignal --release -- -g 1000000 -e 10 -a -r optd-orig
```

Add a 10K expression DAG, triggering group merges, to an in-memory memo (from optd-original) and benchmark rule matching:
```
cargo run --features=optd-orignal --release -- -g 1000 -d -e 10 -a -m  -u merge optd-orig
```

Add a 10K expression DAG to Redis and benchmark retrieval and rule matching (the server needs to be running on localost):
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

The benchmark has sub-commands for each implementation of the memo data structure. Currently, optd-orig (stored in memory, from optd-original), optd-db (stored in SQLite with sqlx, from optd), redis (a simple implementation using Redis), and calcite (implemented in Java and stored in memory by Apache Calcite).

There are currently three stages: add (that generates data and populates the memo), retrieve (that does lookups on a populated memo (and implicitly checks that it has been correctly inserted), and match (that simulates matching a single rule against the current memo).

Data generation and the add stage have several options:

- Select target number of groups and expressions in each group.
- Tree mode (default), generating a structure that looks like a relational expression inserted at the start, or DAG mode, generating a structure with multiple equivalent expressions in each group that mimics the state of the memo after optimization is running for some time. The tree mode works best with -e1.
- Set a custom target number of groups and average expressions per group. The final result might have a slightly different number of expressions and groups due to randomness and to avoid dangling expressions.
- Use a custom seed to repeat a given run. This allows running the exact same data on multiple implementations and reproducible debugging.
- Shuffling the groups. By default, groups are inserted sequentially, which does not really exercise the memo. The lookup mode requires that the memo returns existing duplicate expressions, but does not trigger group merges. The merge mode makes sure that group merges (and recursive group merges) are needed. The latter is likely to be more useful as a torture test than as a benchmark.

There are some additional options for retrieving results:

- Dump the generated data to a GraphViz file.
- Dump the insertion order to a CSV file, mainly for debugging.
- Print benchmarking results in CSV format to stdout.

See command-line help for syntax using --help for general options and --help on each subcommand for implementation-specific options (e.g., database connection strings).
