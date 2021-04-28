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
    print("prepare data")
    instance.query("""
CREATE TABLE default.store1
(
    `source` String,
    `sourcetype` String,
    `host` String,
    `_raw` String,
    `_time` DateTime64(3)
)
ENGINE = MergeTree
ORDER BY _time"""
                   )


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    print("setup node")
    try:
        cluster.start()
        prepare_data()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("table, query, status", [
    (
        "store1",
        {
            "enrichment": {
                "time_extraction_type": "json_path",
                "time_extraction_rule": "log.time"
            },
            "data": [{
                "source": "dae-access-log",
                "sourcetype": "log",
                "host": "host1",
                "_raw": "{\"log\":{\"time\":\"2021-03-20 00:12:23\"}}"
            }, {
                "source": "dae-access-log",
                "sourcetype": "log",
                "_raw": "{\"log\":{\"time\":\"2021-03-20 00:12:23\"}}"
            }]
        }, {
            "status": 200,
            "result": ["query_id", "poll_id"]
        }
    ),
    (
        "store1",
        {
            "enrichment": {
                "time_extraction_type": "regex",
                "time_extraction_rule": "^(?P<_time>.+),\\s+\\[\\w+\\]"
            },
            "data": [
                {"_raw": "2021-03-21 00:10:23, [Apache] This is a error.", "host": "host1"},
                {"_raw": "2021-03-22 00:10:23, [Apache] This is a error."},
                {"_raw": "2021-03-23 00:10:23, [Apache] This is a error."}
            ]
        }, {
            "status": 200,
            "result": ["query_id", "poll_id"]
        }
    ),
    (
        "store1",
        {
            "enrichment": {
                "time_extraction_type": "regex",
                "time_extraction_rule": "^(?P<_time1>.+),\\s+\\[\\w+\\]"
            },
            "data": [
                {"_raw": "2021-03-21 00:10:23, [Apache] This is a error.", "host": "host1"},
                {"_raw": "2021-03-22 00:10:23, [Apache] This is a error."},
                {"_raw": "2021-03-23 00:10:23, [Apache] This is a error."}
            ]
        }, {
            "status": 500,
            "result": ["552", "DB::Exception: No '_time' group defined", "request_id"]
        }
    ),
    (
        "store1",
        {
            "enrichment": {
                "time_extraction_type": "json_path",
                "time_extraction_rule": "log.time1"
            },
            "data": [{
                "source": "dae-access-log",
                "sourcetype": "log",
                "host": "host1",
                "_raw": "{\"log\":{\"time\":\"2021-03-20 00:12:23\"}}"
            }, {
                "source": "dae-access-log",
                "sourcetype": "log",
                "_raw": "{\"log\":{\"time\":\"2021-03-20 00:12:23\"}}"
            }]
        }, {
            "status": 400,
            "result": ["117", "extract _time from _raw failed with rule: log.time1", "request_id"]
        }
    )
])
def test_rawstore_ingest_api(table, query, status):
    instance.ip_address = "localhost"
    # insert data
    resp = instance.http_request(method="POST", url="dae/v1/ingest/default/rawstores/" + table, data=json.dumps(query))
    content = resp.text
    assert resp.status_code == status["status"]
    for keyword in status["result"]:
        assert keyword in content

    if status["status"] == 200:
        rows = instance.http_query(f"SELECT * from {table} limit 1")
        # cols[4] is _time
        cols = rows.split('\t')
        assert cols[4]
    # clean up
    instance.http_query(sql=f"TRUNCATE table {table}", data=" ")
