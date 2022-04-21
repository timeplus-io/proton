from cgi import test
import os, sys, json, getopt, subprocess, traceback
import logging, logging.config
import time
import datetime
import random
import requests
import multiprocessing as mp
from clickhouse_driver import Client




url = "http://localhost:3218/proton/v1/ddl/streams"

def list_stream(id, retry):
    i = 0
    while i < retry:
        try:
            res = requests.get(url)
            if res.status_code != 200:
                sys.exit(1)
            print(f"id = {id}, res.status_code = {res.status_code}")
            time.sleep(random.random())
            i += 1
        except (BaseException) as error:
            print(f"requests.get({url} exception,error ={error}, res.json() = {res.json()}")
            sys.exit(1)


def drop_and_create():
    client = Client('localhost', port=8463)
    drop_sql = "drop stream if exists test_topmax"
    create_sql = "create stream if not exists test_topmax (uuid uuid, int int, uint uint8, string string, float float, decimal decimal32(3), date date, datetime datetime, enum enum('a'=1, 'b'=2, 'z'=26), tuple tuple(s string, i int), ipv4 ipv4, ipv6 ipv6, map map(string, int), nullable nullable(datetime64), timestamp datetime64(3) default now64(3))"
    rename_sql1 = "rename stream if exists test_topmax to aaa"
    rename_sql2 = "rename stream if exists aaa to test_topmax"
    
    while True:
        client.execute(drop_sql)
        client.execute(create_sql)
        client.execute(rename_sql1)
        client.execute(rename_sql2)


if __name__ == "__main__":
    
    procs = []
    for i in range(40):
        proc = mp.Process(target=list_stream, args=(i,200,))
        proc.start()
        procs.append(proc)
    
    proc_1 = mp.Process(target=drop_and_create)
    proc_1.start()
    procs.append(proc_1)
    
    for proc in procs:
        proc.join()
    
    print("done")
