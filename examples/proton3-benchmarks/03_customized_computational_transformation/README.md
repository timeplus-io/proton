## Benchmark – Customized computational transformation (JS UDF)

This case reproduces the JavaScript UDF telemetry enrichment benchmark.

- SQL script: `js_telemetry_log_enrich_demo.sql`

The script:

- Creates a synthetic `nginx_access_log` random stream.
- Defines the `nginx_log_enrich_js` JavaScript scalar UDF for enrichment and anomaly scoring.
- Creates the `nginx_log_enriched_js_v` view.
- Includes the example perf query used in the blog.

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

3. In the client, execute `js_telemetry_log_enrich_demo.sql` to create the stream, UDF, view, and run the perf query.
