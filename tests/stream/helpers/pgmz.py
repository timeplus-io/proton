import psycopg
import os, sys, getopt, json, random
from re import sub
import logging, logging.config
from proton_driver import Client
from proton_driver import errors
import csv
import datetime
import time
import requests
import multiprocessing as mp
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import rockets
from helpers.rockets import env_setup
from helpers.rockets import table_exist
from helpers.rockets import create_table_rest
from helpers.rockets import find_schema
from helpers.rockets import drop_table_if_exist_rest
from helpers.rockets import find_table_reset_in_table_schemas


POSTGRES_USER = os.getenv("PGUSER", "materialize")
POSTGRES_HOST = os.getenv("PGHOST", "localhost")
POSTGRES_PORT = os.getenv("PGPORT", 6875)
POSTGRES_PASSWORD = os.getenv("PGPASSWORD", "materialize")



def db_connection(dbname=None):
    conn = psycopg.connect(
        user=POSTGRES_USER,
        host=POSTGRES_HOST,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
        dbname=dbname,
    )
    conn.autocommit = True
    return conn

try:
    conn = db_connection('my_db')
    CAN_CONNECT_TO_DB = True
    SERVER_VERSION = conn.server_version
except(BaseException) as error:
    print(f"exception error = {error}")
    SERVER_VERSION = 0


print(f"SERVER_VERSION = {SERVER_VERSION}")
   




'''

with psycopg.connect("host=localhost port=6875 dbname=my_db user=materialize") as conn:
    with conn.cursor() as cur:
        cur.execute("""
            select * from test
        """)
        for record in cur:
            print(record)
        
    conn.commit()
'''