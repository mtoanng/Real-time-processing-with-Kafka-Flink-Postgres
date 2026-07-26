# `behavior-rules` topic

> Deprecated legacy extension. Retained for compatibility only; not a current
> release target.

The Debezium connector routes `public.behavior_rules` changes to
`behavior-rules`. Create it with `cleanup.policy=compact`; the record key is the
PostgreSQL primary key `rule_id`, so the latest version of each rule survives
compaction. Values use the fields in `schemas/behavior-rule.avsc` after the
Debezium unwrap transform.

The topic is created by `scripts/create_behavior_rules_topic.sh`. This command is
remote-only and requires an explicit Kafka bootstrap address.
