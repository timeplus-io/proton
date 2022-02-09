import logging
import os, sys, datetime, time, json, csv

import kafka
import psycopg2
import psycopg2.errorcodes
import psycopg2.extensions as ext
import psycopg2.extras
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE, make_dsn

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(processName)s %(module)s %(funcName)s %(message)s"
)
console_handler = logging.StreamHandler(sys.stderr)
console_handler.formatter = formatter
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


POSTGRES_USER = os.getenv("MZUSER", os.getenv("PGUSER", "materialize"))
POSTGRES_HOST = os.getenv("MZHOST", os.getenv("PGHOST", "localhost"))
POSTGRES_PORT = os.getenv("MZPORT", os.getenv("PGPORT", 6875))
POSTGRES_PASSWORD = os.getenv("MZPASSWORD", os.getenv("PGPASSWORD", "materialize"))


def db_connection(dbname=None):
    conn = psycopg2.connect(
        user=POSTGRES_USER,
        host=POSTGRES_HOST,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
        database=dbname,
    )
    conn.autocommit = True
    return conn


def create_db(dbname):
    with db_connection().cursor() as cur:
        try:
            cur.execute(f"""CREATE DATABASE {dbname}""")
        except:
            pass


def drop_tables(conn):
    with conn.cursor() as cur:
        # Materialize does not support dropping these items in transactions, so
        # we need to send them as individual queries.
        commands = """\
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            DROP SCHEMA IF EXISTS schema1 CASCADE;
            DROP SCHEMA IF EXISTS schema2 CASCADE;\
        """.split(
            "\n"
        )
        for cmd in commands:
            cur.execute(cmd.strip())


"""copy past from neutron/docker/integration/notebooks/Sprint-8 Proton KSQL Materialize.ipynb"""


def kafka_delete_topic(brokers, topic_prefix):
    admin = kafka.KafkaAdminClient(bootstrap_servers=brokers)
    consumer = kafka.KafkaConsumer(bootstrap_servers=brokers)
    for topic in consumer.topics():
        if topic.startswith(topic_prefix):
            try:
                admin.delete_topics([topic])
            except Exception as e:
                print(e)


def kafka_get_topic(brokers, topic_prefix):
    consumer = kafka.KafkaConsumer(bootstrap_servers=brokers)
    for topic in consumer.topics():
        if topic.startswith(topic_prefix):
            return topic
    return None



class MaterializeDB:
    def __init__(
        self,
        host="localhost",
        port=6875,
        user="materialize",
        db="materialize",
        broker="kafka:9092",
        query_log_csv=None,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.db = db
        self.broker = broker
        self.query_log = query_log_csv

    def sql(self, sql):
        conn = psycopg2.connect(
            f"host={self.host} port={self.port} dbname={self.db} user={self.user}"
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
        finally:
            if conn:
                conn.close()

    def query(self, sql):
        conn = psycopg2.connect(
            f"host={self.host} port={self.port} dbname={self.db} user={self.user}"
        )
        with conn.cursor() as cursor:
            query = sql
            cursor.execute(query)
            while True:
                try:
                    row = cursor.fetchone()
                    if row == None:
                        break
                    print(row)
                except Exception as e:
                    print(e)

    def query_to_kafka(self, sql, timeout=5, time_col=None, show_data=False):
        view_name = "temp_view"
        creat_view_sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS {sql}"
        drop_view_sql = f"DROP VIEW IF EXISTS {view_name}"

        sink_name = "temp_sink_m"
        broker = self.broker

        # delete topic
        kafka_delete_topic(broker, sink_name)

        drop_sink_sql = f"DROP SINK IF EXISTS {sink_name}"
        create_sink_sql = f"""CREATE SINK IF NOT EXISTS {sink_name}
                FROM {view_name}
                INTO KAFKA BROKER '{broker}' TOPIC '{sink_name}'
                FORMAT JSON;"""

        kafka_delete_topic(broker, sink_name)

        try:
            self.sql(drop_sink_sql)
        except Exception as e:
            print(e)

        try:
            self.sql(drop_view_sql)
        except Exception as e:
            print(f"failed to drop view/sink {e}")

        try:
            self.sql(creat_view_sql)
        except Exception as e:
            print(f"failed to create view {e}")

        try:
            self.sql(create_sink_sql)
        except Exception as e:
            print(f"failed to create sink {e}")

        sink_topic = kafka_get_topic(broker, sink_name)
        print(f"view to sink {sink_topic}")



    def stream_query(
        self,
        sql,
        timeout=5,
        time_col=None,
        time_col_index=-1,
        show_data=False,
        query_log_csv=None,
        query_kill_semaphore=None,
    ):
        view_name = "temp_view"
        creat_view_sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS {sql}"
        drop_view_sql = f"DROP VIEW IF EXISTS {view_name}"
        self.sql(creat_view_sql)
        self.tail_view(
            view_name,
            timeout=timeout,
            time_col=time_col,
            time_col_index=time_col_index,
            show_data=show_data,
            query_log_csv=query_log_csv,
            query_kill_semaphore=query_kill_semaphore,
        )
        self.sql(drop_view_sql)

    def tail_view(
        self,
        view,
        timeout=5,
        time_col=None,
        time_col_index=-1,
        show_data=False,
        query_log_csv=None,
        query_kill_semaphore=None,
    ):
        conn = psycopg2.connect(
            f"host={self.host} port={self.port} dbname={self.db} user={self.user}"
        )
        with conn.cursor() as cursor:
            query = f"DECLARE cur CURSOR FOR TAIL {view} WITH (PROGRESS)"
            cursor.execute(query)
            stream = self.tail_view_inner(
                cursor,
                timeout=timeout,
                time_col=time_col,
                time_col_index=time_col_index,
                show_data=show_data,
                query_log_csv=query_log_csv,
                query_kill_semaphore=query_kill_semaphore,
            )

    def tail_view_inner(
        self,
        cursor,
        timeout=600,
        time_col=None,
        time_col_index=-1,
        show_data=False,
        query_log_csv=None,
        query_kill_semaphore=None,
    ):
        start_time = time.time()
        latencys = []
        while True and query_kill_semaphore.value != True:
            current_time = time.time()
            logger.debug(str(int(current_time - start_time)))
            if int(current_time - start_time) > timeout:
                break
            if query_log_csv != None:
                writer = csv.writer(query_log_csv)
            cursor.execute(f"FETCH ALL cur")
            for item in cursor:
                logger.debug(f"item in cursor = {item}")
                if isinstance(item, list) or isinstance(item, tuple):
                    item = list(item)
                    item.append(str(datetime.datetime.now()))
                if query_log_csv:
                    writer.writerow(item)

                if show_data:
                    print(item)


if __name__ == "__main__":
    kafka_broker = "kafka:9092"
    mz = MaterializeDB()
    for source in [
        "car_live_data",
        "bookings",
        "credit_change",
        "trips",
        "dim_car_info",
        "dim_user_info",
    ]:
        create_source_sql = f"CREATE SOURCE IF NOT EXISTS {source} FROM KAFKA BROKER '{kafka_broker}' TOPIC '{source}' WITH (start_offset=0) FORMAT BYTES;"
        mz.sql(create_source_sql)

    create_car_live_data_view_sql = """CREATE MATERIALIZED VIEW IF NOT EXISTS car_live_data_view AS
SELECT
    (data->>'_emit_time') AS _emit_time,
    (data->>'cid') AS cid,
    (data->>'gas_percent')::float AS gas_percent,
    (data->>'in_use')::boolean AS in_use,
    CAST((data->>'time') as timestamp) AS time,
    (data->>'latitude')::float AS latitude,
    (data->>'longitude')::float AS longitude,
    (data->>'locked')::boolean AS locked,
    (data->>'speed_kmh')::int AS speed_kmh,
    (data->>'total_km')::float AS total_km
FROM (
    SELECT CAST(data AS JSONB) AS data
    FROM (
        SELECT CONVERT_FROM(data, 'utf8') AS data
        FROM car_live_data
    )
)"""

    mz.sql(create_car_live_data_view_sql)
    creat_table_dim_user_info_sql = """CREATE TABLE IF NOT EXISTS dim_user_info_table
(_emit_time text, gender text, birthday text, uid text, first_name text, 
last_name text, email text, credit_card text);"""

    mz.sql(creat_table_dim_user_info_sql)
    query_res = mz.query("select * from dim_user_info_table")
    query_res = mz.stream_query("select * from dim_user_info_table", show_data=True)
    print(f"query_res = {query_res}")

    print("Done...")
