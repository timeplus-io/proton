
This demo shows how to read and write Iceberg tables using Proton.

Quick start:
1. `cd examples/iceberg`
2. `docker compose up -d`
3. Open `http://localhost:8888` and run `notebooks/IcebergPythonTest.ipynb` to create and populate the Iceberg table.
4. Run the SQL script from your host:
   `docker compose exec -T proton proton-client --multiquery --user proton --password 'proton@t+' < script/proton.sql`
5. Optional (interactive): `docker compose exec proton proton-client --user proton --password 'proton@t+'`
6. Re-run the notebook cell that reads the Iceberg table to see the new rows.

Notes:
- `storage_endpoint` stays as `s3://warehouse/` because HTTP endpoints are rewritten to `s3://<host>/...` and would point at the wrong bucket.
- Jupyter binds to `127.0.0.1`; change the port mapping if you need remote access.
