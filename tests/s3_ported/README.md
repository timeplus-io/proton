# S3 Test Run Guide

This document describes how to run S3 tests in a local environment.

## 1. Configure MinIO

First, navigate to the `s3_ported` directory and run the script to configure MinIO.

```bash
cd s3_ported
./setup_minio.sh start
```

This script will start the MinIO service for use in subsequent tests.

You can also stop the MinIO service and delete its data.

```bash
./setup_minio.sh stop   # Stop the MinIO service
./setup_minio.sh clean  # Remove MinIO data
```

## 2. Start the Proton Server

After configuring MinIO, you need to start the `proton-server` and specify the appropriate configuration file.

```bash
/path/to/proton-server -C tests/s3_ported/config/config.xml
```

Replace `/path/to/` with the actual path of your `proton-server`.

## 3. Run the S3 Test

Run the Python script `ported-clickhouse-test.py` located in the `tests` directory to perform the test.

```bash
cd tests
CLICKHOUSE_CLIENT=../build/programs/proton-client python ./ported-clickhouse-test.py -c ../build/programs/proton-client -q s3_ported --database default
```

### Parameter Explanation:
- `-c` specifies the path to the `proton-client`.
- `-q s3_ported` refers to the directory for the S3 test.
- `--database default` sets the database to `default`. Without this, the script will create temporary databases for each test case, but it may lack the necessary permissions.

Once the above commands are executed, the test results will be generated and displayed.