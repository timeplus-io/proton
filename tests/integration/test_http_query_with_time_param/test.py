import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')

def prepare_data():
    print("prepare data")
    instance.query("""
    CREATE TABLE A(
        _time DateTime64,
        n UInt64
        ) 
    ENGINE = MergeTree() ORDER BY _time
    """
    )
    instance.query("""
    CREATE TABLE B(
        _time DateTime64,
        n UInt64
        ) 
    ENGINE = MergeTree() 
    ORDER BY _time
    """
    )
    for d in range(0, 10):
        for i in range(10):
            instance.query("""
            INSERT INTO A(_time, n) VALUES('2021-01-2{date}', 123)
            """.format(date=d))
    for d in range(0, 5):
        for i in range(5):
            instance.query("""
            INSERT INTO B(_time, n) VALUES('2021-01-2{date}', 456)
            """.format(date=d))

@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    print("setup node")
    try:
        cluster.start()
        prepare_data()
        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize("origin_query, new_query", [
    (
        "SELECT count(1) FROM A "
        "WHERE _time >= '2021-01-20 00:00:00.000' "
        "AND _time < '2021-01-25 00:00:00.000'",
        {
            "query": "SELECT count(1) FROM A",
            "params":{
                "time_start": "'2021-01-20'",
                "time_end": "'2021-01-25'"
            }
        }
    ),
    (
        "SELECT * FROM (SELECT count(1) FROM A "
        "WHERE _time >= '2021-01-20 00:00:00.000' "
        "AND _time < '2021-01-25 00:00:00.000')",
        {
            "query": "SELECT * FROM (SELECT count(1) FROM A)",
            "params":{
                "time_start": "'2021-01-20'",
                "time_end": "'2021-01-25'"
            }
        }
    ),
    (
        "SELECT * FROM (SELECT count(1) FROM A, B "
        "WHERE A._time >= '2021-01-20 00:00:00.000' "
        "AND A._time < '2021-01-25 00:00:00.000')",
        {
            "query": "SELECT * FROM (SELECT count(1) FROM A, B)",
            "params":{
                "time_start": "'2021-01-20'",
                "time_end": "'2021-01-25'"
            }
        }
    ),
    (
        "SELECT * FROM "
            "("
                "SELECT count(1) FROM "
                    "("
                        "SELECT * FROM A "
                        "WHERE A._time >= '2021-01-20 00:00:00.000' "
                        "AND A._time < '2021-01-25 00:00:00.000'"
                    ") A1,"
                "B"
            ")",
        {
            "query": "SELECT * FROM (SELECT count(1) FROM (SELECT * FROM A) A1, B)",
            "params":{
                "time_start": "'2021-01-20'",
                "time_end": "'2021-01-25'"
            }
        }
    ),
    (
        "SELECT count(1) FROM (SELECT _time FROM A "
        "WHERE _time >= '2021-01-20 00:00:00.000' AND _time < '2021-01-25 00:00:00.000' "
        "UNION ALL "
        "SELECT _time FROM B "
        "WHERE _time >= '2021-01-20 00:00:00.000' AND _time < '2021-01-25 00:00:00.000')",
        {
            "query": "SELECT count(1) FROM (SELECT _time FROM A UNION ALL SELECT _time FROM B)",
            "params":{
                "time_start": "'2021-01-20'",
                "time_end": "'2021-01-25'"
            }
        }
    ),
    (
        "SELECT count(1) FROM (SELECT * FROM A "
        "WHERE _time >= '2021-01-21 00:00:00.000' AND _time < '2021-01-23 00:00:00.000')",
        {
            "query": "SELECT count(1) FROM (SELECT _time FROM A "
                     "WHERE _time >= '2021-01-20 00:00:00.000' AND _time < '2021-01-25 00:00:00.000')",
            "params":{
                "time_start": "'2021-01-21'",
                "time_end": "'2021-01-23'"
            }
        }
    )
])
def test_http_query_with_time_param(origin_query, new_query):
    origin_res = instance.http_query(origin_query)
    new_res = instance.http_query(new_query['query'], params=new_query['params'])
    assert origin_res == new_res
