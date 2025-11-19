## Benchmark – Stateful CDC aggregations

This case reproduces the changelog CDC benchmark that maintains account balances from a high‑volume transfer stream.

- SQL script: `stateful_cdc_aggregations.sql`

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

3. In the client, execute the statements from `stateful_cdc_aggregations.sql` to create the changelog stream, random source, materialized view, and streaming aggregation query.
