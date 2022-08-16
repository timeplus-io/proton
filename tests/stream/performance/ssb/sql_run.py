import argparse, datetime, time
from clickhouse_driver import Client


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("sql", help="the sql file to be loaded")
    args = parser.parse_args()
    sql = args.sql

    client = Client('localhost', port= 8463)
    start_at = datetime.datetime.now()
    res = client.execute(sql)
    end_at = datetime.datetime.now()
    duration = end_at - start_at
    print(f"sql = {sql}")
    print(f"sql execution starts at {start_at}")
    print(f"sql execution ends at {end_at}")
    print(f"sql execution takes {duration.total_seconds()*1000} ms")
    print(f"sql execution results = {res}")

