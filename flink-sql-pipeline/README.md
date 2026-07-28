# Flink SQL pipeline

This is the active Data Engineer authoring surface.

- `pipeline.sql.template` contains business transformations.
- `render.py` validates configuration and renders one submission file.
- Maven only packages prebuilt Kafka/Avro JVM connector classes. There is no
  project-authored Java source in this module.

The former Java DataStream implementation under `flink-jobs/` is retained as
non-active migration evidence and is not part of the root Maven reactor.
