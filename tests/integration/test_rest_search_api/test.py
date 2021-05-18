import json

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                main_configs=['configs/kafka.xml'],
                                with_kafka=True,
                                with_zookeeper=True,
                                )


def prepare_data():
    instance.ip_address = "localhost"
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
        "AND _time < '2021-01-25 00:00:00.000'"
        "LIMIT 0, 10000 "
        "FORMAT JSONCompactEachRowWithNamesAndTypes ",
        {
            "query": "SELECT count(1) FROM A",
            "offset": 0,
            "page_size": 10000,
            "start_time": "'2021-01-20'",
            "end_time": "'2021-01-25'",
            "mode": "standard"
        }
    ),
    (
        "SELECT * FROM (SELECT count(1) FROM A "
        "WHERE _time >= '2021-01-20 00:00:00.000' "
        "AND _time < '2021-01-25 00:00:00.000') "
        "LIMIT 0, 10 "
        "FORMAT JSONCompactEachRowWithNamesAndTypes ",
        {
            "query": "SELECT * FROM (SELECT count(1) FROM A)",
            "offset": 0,
            "page_size": 10,
            "start_time": "'2021-01-20'",
            "end_time": "'2021-01-25'",
            "mode": "standard"
        }
    ),
    (
        "SELECT * FROM (SELECT count(1) FROM A, B "
        "WHERE A._time >= '2021-01-20 00:00:00.000' "
        "AND A._time < '2021-01-25 00:00:00.000')"
        "LIMIT 10, 10 "
        "FORMAT JSONCompactEachRowWithNamesAndTypes",
        {
            "query": "SELECT * FROM (SELECT count(1) FROM A, B)",
            "offset": 10,
            "page_size": 10,
            "start_time": "'2021-01-20'",
            "end_time": "'2021-01-25'",
            "mode": "verbose"
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
        ") "
        "LIMIT 0, 10000 "
        "FORMAT JSONCompactEachRowWithNamesAndTypes ",
        {
            "query": "SELECT * FROM (SELECT count(1) FROM (SELECT * FROM A) A1, B)",
            "offset": 0,
            "page_size": 10000,
            "start_time": "'2021-01-20'",
            "end_time": "'2021-01-25'",
            "mode": "verbose"
        }
    ),
    (
        "SELECT count(1) FROM (SELECT _time FROM A "
        "WHERE _time >= '2021-01-20 00:00:00.000' AND _time < '2021-01-25 00:00:00.000' "
        "UNION ALL "
        "SELECT _time FROM B "
        "WHERE _time >= '2021-01-20 00:00:00.000' AND _time < '2021-01-25 00:00:00.000') "
        "LIMIT 0, 10000 "
        "FORMAT JSONCompactEachRowWithNamesAndTypes ",
        {
            "query": "SELECT count(1) FROM (SELECT _time FROM A UNION ALL SELECT _time FROM B)",
            "offset": 0,
            "page_size": 10000,
            "start_time": "'2021-01-20'",
            "end_time": "'2021-01-25'",
            "mode": "standard"
        }
    ),
    (
        "SELECT count(1) FROM (SELECT * FROM A "
        "WHERE _time >= '2021-01-21 00:00:00.000' AND _time < '2021-01-23 00:00:00.000') "
        "LIMIT 0, 10000 "
        "FORMAT JSONCompactEachRowWithNamesAndTypes ",
        {
            "query": "SELECT count(1) FROM (SELECT _time FROM A "
                     "WHERE _time >= '2021-01-20 00:00:00.000' AND _time < '2021-01-25 00:00:00.000')",
            "offset": 0,
            "page_size": 10000,
            "start_time": "'2021-01-21'",
            "end_time": "'2021-01-23'",
            "mode": "verbose"
        }
    )
])
def test_search_api(origin_query, new_query):
    instance.ip_address = "localhost"
    origin_res = instance.http_query(origin_query)
    new_res = instance.http_request(method="POST", url="dae/v1/search", data=json.dumps(new_query))
    assert origin_res == new_res.text


@pytest.mark.parametrize("query, status", [
    (
        {
            "query": "select count(1) from A WHERE _time > '2020-12-12 00:00:00'",
            "offset": -1,
            "page_size": 10000,
            "start_time": "'2020-01-01'",
            "end_time": "now()",
            "mode": "standard"
        }, {
            "status": 400,
            "result": ["request_id", "408", "Invalid 'limit' or 'page_size"]
        }
    ),
    (
        {
            "query": "select count(1) from A WHERE _time > '2020-12-12 00:00:00'",
            "offset": 0,
            "page_size": 0,
            "start_time": "'2020-01-01'",
            "end_time": "now()",
            "mode": "standard"
        },
        {
            "status": 400,
            "result": ["request_id", "408", "Invalid 'limit' or 'page_size"]
        }
    ),
    (
        {
            "query": "select count(1) from A WHERE _time > '2020-12-12 00:00:00'",
            "offset": 0,
            "start_time": "'2020-01-01'",
            "end_time": "now()",
            "mode": "standard"
        },
        {
            "status": 400,
            "result": ["request_id", "408", "Missing 'limit' or 'page_size"]
        }
    ),
    (
        {
            "query": "select count(1) from A WHERE _time > '2020-12-12 00:00:00'",
            "start_time": "'2020-01-01'",
            "end_time": "now()",
            "mode": "real"
        },
        {
            "status": 400,
            "result": ["request_id", "408", "Invalid 'mode'"]
        }
    ),
    (
        {
            "offset": 0,
            "start_time": "'2020-01-01'",
            "end_time": "now()",
            "mode": "standard"
        },
        {
            "status": 400,
            "result": ["request_id", "408", "Required param 'query' is missing"]
        }
    ),
    (
        {
            "query": "insert into A values ('2021-01-01 00:00:00', 789)",
        },
        {
            "status": 500,
            "result": ["request_id", "62"]
        }
    )

])
def test_search_api_error(query, status):
    # instance.ip_address = "localhost"
    resp = instance.http_request(method="POST", url="dae/v1/search", data=json.dumps(query))
    content = resp.text
    assert resp.status_code == status["status"]
    for keyword in status["result"]:
        assert keyword in content
