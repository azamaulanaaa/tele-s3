# Benchmark guide

This crate uses Criterion for hot-path benchmarks.

## Run

```sh
cargo bench --bench memory_backend
```

Smoke build:

```sh
cargo bench --bench memory_backend -- --test
```

CI runs `cargo test --all-targets`, which builds the benchmark targets.
EOF