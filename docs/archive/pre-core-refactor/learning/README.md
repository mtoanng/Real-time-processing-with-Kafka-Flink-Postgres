# Project 1 Learning Package

> Archived learning package for the pre-core-refactor architecture.

This package explains the repository as it exists. The active operational
serving technology is Apache Cassandra with DataStax Astra DB Serverless as
the managed target. Older ScyllaDB names are historical migration material,
not an active implementation path.

## Contents

- [Layer 1: Architecture](01_ARCHITECTURE.md)
- [Layer 2: Code and Data Trace](02_CODE_WALKTHROUGH.md)
- [Interview Guide](03_INTERVIEW_GUIDE.md)
- [Failures, Trade-offs, and Claims](04_FAILURES_TRADEOFFS.md)

The separate [Flink API strategy](../FLINK_API_STRATEGY.md) explains the
DataStream choice without adding a second SQL pipeline.

## Student Task

Draw the core path and explain why the same event stream has two different
serving responsibilities: ClickHouse owns history and aggregates, while
Apache Cassandra serves one user’s active-cart items.
