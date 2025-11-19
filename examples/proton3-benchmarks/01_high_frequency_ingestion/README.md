## Benchmark – High-frequency ingestion

This case reproduces the high‑throughput append‑only ingestion benchmark.

- SQL script: `high_frequency_ingestion.sql`

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

3. In the client, execute the statements from `high_frequency_ingestion.sql` in order to create the stream and run the insert benchmark.
