## Proton 3.0 blog benchmarks

Each subfolder corresponds to one benchmark scenario:

- `01_high_frequency_ingestion` – high‑throughput append‑only ingestion benchmark.
- `02_stateful_cdc_aggregations` – changelog CDC stream with stateful aggregations.
- `03_customized_computational_transformation` – JavaScript UDF telemetry enrichment benchmark.
- `04_high_cardinality_single_key_aggregation` – single‑key reduce / high‑cardinality aggregation micro‑benchmark.

### Version comparison

The blog’s performance numbers compare:

- Old: Proton `1.6.17-rc`
- New: Proton `3.0.7`

Run the same SQL against each version separately (for example using different Docker tags or locally built binaries) to reproduce the comparison.

To run any scenario:

1. Start Proton (e.g. with Docker; see the per-scenario README for copy‑paste commands).
2. Connect with `proton client`.
3. Execute the SQL script(s) from the corresponding scenario directory in your client session.
