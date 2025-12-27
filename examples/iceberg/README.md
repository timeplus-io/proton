
this demo shows how to read/write iceberg table using proton.

quick start:
1. run `docker compose up` to start the whole stack
2. open `localhost:8888` from the broswer and run all the python code that create iceberg table and write read data from `IcebergPythonTest` notebook
3. run `proton-client --user proton --password proton@t+` to start a proton client cli in the proton container
4. run all the script in `script/proton.sql` to query the iceberg table and write some data into iceberg table
5. in the notebook, rerun the cell that read the iceberg table and check the newly inserted data from proton.