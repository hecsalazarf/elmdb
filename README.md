# elmdb

[LMDB](http://www.lmdb.tech/doc/) for Rust with extended features and an idiomatic API.

## Features

- Thread-safe LMDB environment protected by an `Arc`.
- Read-write transactions created asynchronously (with Tokio runtime).
- Idempotent read-write transactions.
- Access control to Environment's via `Manager`.
- Typed key-value store powered by serde.
- Persistent data structures: Sorted sets and queues at the moment.

## Build

```bash
cargo build
```

## Test

```bash
cargo test
```
## Documentation

```bash
cargo doc --no-deps --open
```
