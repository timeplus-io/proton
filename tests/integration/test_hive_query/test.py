import logging
import os

import time
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance('h0_0_0', main_configs=['configs/config.xml'], extra_configs=[ 'configs/hdfs-site.xml'], with_hive=True)
        
        logging.info("Starting cluster ...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_create_parquet_table(started_cluster):
    logging.info('Start testing creating hive table ...')
    node = started_cluster.instances['h0_0_0']
    node.query("set input_format_parquet_allow_missing_columns = true")
    result = node.query("""
    DROP TABLE IF EXISTS default.demo_parquet;
    CREATE TABLE default.demo_parquet (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
            """)
    logging.info("create result {}".format(result))
    time.sleep(120)
    assert result.strip() == ''

def test_create_orc_table(started_cluster):
    logging.info('Start testing creating hive table ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    DROP TABLE IF EXISTS default.demo_orc;
    CREATE TABLE default.demo_orc (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_orc') PARTITION BY(day)
            """)
    logging.info("create result {}".format(result))
    
    assert result.strip() == ''

def test_create_text_table(started_cluster):
    logging.info('Start testing creating hive table ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    DROP TABLE IF EXISTS default.demo_text;
    CREATE TABLE default.demo_text (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_text') PARTITION BY (tuple())
            """)
    logging.info("create result {}".format(result))
    
    assert result.strip() == ''

def test_parquet_groupby(started_cluster):
    logging.info('Start testing groupby ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result
def test_orc_groupby(started_cluster):
    logging.info('Start testing groupby ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    SELECT day, count(*) FROM default.demo_orc group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result

def test_text_count(started_cluster):
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    SELECT day, count(*) FROM default.demo_orc group by day order by day SETTINGS format_csv_delimiter = '\x01'
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result

def test_parquet_groupby_with_cache(started_cluster):
    logging.info('Start testing groupby ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result
def test_cache_read_bytes(started_cluster):
    node = started_cluster.instances['h0_0_0']
    node.query("set input_format_parquet_allow_missing_columns = true")
    result = node.query("""
    DROP TABLE IF EXISTS default.demo_parquet;
    CREATE TABLE default.demo_parquet (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
            """)
    result = node.query("""
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """)
    result = node.query("""
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    time.sleep(120)
    assert result == expected_result
    result = node.query("select sum(ProfileEvent_ExternalDataSourceLocalCacheReadBytes)  from system.metric_log where ProfileEvent_ExternalDataSourceLocalCacheReadBytes > 0")
    logging.info("Read bytes from cache:{}".format(result))
    assert result.strip() != '0'


def test_cache_dir_use(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result0 = node.exec_in_container(
        ["bash", "-c", "ls /tmp/clickhouse_local_cache | wc -l"]
    )
    result1 = node.exec_in_container(
        ["bash", "-c", "ls /tmp/clickhouse_local_cache1 | wc -l"]
    )
    assert result0 != "0" and result1 != "0"


def test_hive_struct_type(started_cluster):
    node = started_cluster.instances["h0_0_0"]
    result = node.query(
        """
        CREATE TABLE IF NOT EXISTS default.test_hive_types (`f_tinyint` Int8, `f_smallint` Int16, `f_int` Int32, `f_integer` Int32, `f_bigint` Int64, `f_float` Float32, `f_double` Float64, `f_decimal` Float64, `f_timestamp` DateTime, `f_date` Date, `f_string` String, `f_varchar` String, `f_char` String, `f_bool` Boolean, `f_array_int` Array(Int32), `f_array_string` Array(String), `f_array_float` Array(Float32), `f_map_int` Map(String, Int32), `f_map_string` Map(String, String), `f_map_float` Map(String, Float32), `f_struct` Tuple(a String, b Int32, c Float32, d Tuple(x Int32, y String)), `day` String) ENGINE = Hive('thrift://hivetest:9083', 'test', 'test_hive_types') PARTITION BY (day)
        """
    )
    result = node.query(
        """
    SELECT * FROM default.test_hive_types WHERE day = '2022-02-20' SETTINGS input_format_parquet_import_nested=1
        """
    )
    expected_result = """1	2	3	4	5	6.11	7.22	8	2022-02-20 14:47:04	2022-02-20	hello world	hello world	hello world	true	[1,2,3]	['hello world','hello world']	[1.1,1.2]	{'a':100,'b':200,'c':300}	{'a':'aa','b':'bb','c':'cc'}	{'a':111.1,'b':222.2,'c':333.3}	('aaa',200,333.3,(10,'xyz'))	2022-02-20"""
    assert result.strip() == expected_result

    result = node.query(
        """
    SELECT day, f_struct.a, f_struct.d.x FROM default.test_hive_types WHERE day = '2022-02-20' SETTINGS input_format_parquet_import_nested=1
        """
    )
    expected_result = """2022-02-20	aaa	10"""
    assert result.strip() == expected_result
