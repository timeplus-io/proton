## Benchmark – High-cardinality analytics (single-key aggregations)

This case reproduces the single‑key aggregation micro‑benchmark, collapsing a 10B‑row synthetic stream into a single key and joining to a tiny dimension.

- SQL script: `high_cardinality_single_key_aggregation.sql`

Steps:

1. Start Proton (for example with Docker):

   ```bash
   docker run -d --pull always -p 8123:8123 -p 8463:8463 --name proton d.timeplus.com/timeplus-io/proton:latest
   ```

2. Open a SQL client shell, either with a local install:

   ```bash
   proton client
   ```

   or inside the container:

   ```bash
   docker exec -it proton proton client
   ```

3. In the client, execute the statement from `high_cardinality_single_key_aggregation.sql` to run the aggregation benchmark.
