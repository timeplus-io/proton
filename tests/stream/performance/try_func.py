from clickhouse_driver import Client
import os, sys
import multiprocessing as mp
import time


def query_func(query_sql):
    client = Client("localhost", port=8463)
    res = client.execute_iter(query_sql)
    for item in res:
        print(i)


if __name__ == "__main__":

    client = Client("localhost", port=8463)

    for i in range(40):
        drop_view_sql = f"drop view if exists iris_global_sum_v{i}"
        client.execute(drop_view_sql)
        print(f"{drop_view_sql}, done")


    for i in range(40):
        create_mv_sql = f"create materialized view iris_global_sum_v{i} as select species, sum(sepal_length) as sum_sepal_length, sum(sepal_width) as sum_sepal_width from iris group by species"
        client.execute(create_mv_sql)
        print(f"{create_mv_sql}, done")

    query_procs = []
    for i in range(40):
        query_sql = f"select * from iris_global_sum_v{i}"
        args = (
            query_sql,
        )
        query_proc = mp.Process(target=query_func, args=args)
        query_proc.start()
        print(f"{query_sql}, proc start done")
        query_procs.append(query_proc)

    for query_proc in query_procs:
        query_proc.join()

    print("done...")
    while True:




        time.sleep(1)
        insert_sql = f"insert into test('sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species', 'perf_event_time', '_perf_row_id', '_perf_ingest_time')values()"