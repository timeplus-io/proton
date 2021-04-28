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
CREATE TABLE default.test
(
    `a` UInt64,
    `b` String,
    `t` DateTime64(3) DEFAULT now(),
    `n.a` Array(UInt64),
    `n.b` Array(String),
    `ip` IPv6 DEFAULT toIPv6('::127.0.0.1'),
    INDEX idx_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY t
    """
                   )
    instance.query("""
CREATE TABLE default.test2
(
    `i` Int32
)
ENGINE = DistributedMergeTree(1, 1, rand())
ORDER BY i
SETTINGS index_granularity = 8192, shard = 0
    """
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
        "test",
        {
            "columns": ["a", "b", "t", "n.a", "n.b", "ip"],
            "data": [[21, "a", "2021-01-01 23:23:00", [30, 31], ["aa", "ab"], "::10.1.1.1"],
                     [22, "b", "2021-01-01 00:00:00", [31, 32], ["aa", "ab"], "::10.1.1.2"],
                     [23, "c", "2021-01-02 00:00:00.000", [33, 34], ["aa", "ab"], "::10.1.1.3"]]

        }, {
            "status": 400,
            "result": "None of poll_id"
        }
    ),
    (
        "test2",
        {
            "columns": ["i"],
            "data": [[21], [30]]
        }, {
            "status": 200,
            "result": """"progress":"""
        }
    )
])
def test_ingest_api_basic_case(table, query, status):
    instance.ip_address = "localhost"
    # insert data
    resp = instance.http_request(method="POST", url="dae/v1/ingest/default/tables/" + table, data=json.dumps(query))
    result = json.loads(resp.content)
    assert 'poll_id' in result
    assert 'query_id' in result
    assert 'channel' in result
    # get status
    req = {"channel": result['channel'], "poll_ids": [result['poll_id']]}
    resp = instance.http_request(method="POST", url="dae/v1/ingest/statuses", data=json.dumps(req))
    assert resp.status_code == status['status']
    assert status['result'] in resp.text


@pytest.mark.parametrize("poll, status", [
    (
        {
            "channel": "fsfs"
        },
        {
            "status": 404,
            "result": "Unknown channel"
        }
    )
])
def test_status_exception(poll, status):
    instance.ip_address = "localhost"
    # get channel_id
    # get status
    resp = instance.http_request(method="POST", url="dae/v1/ingest/statuses", data=json.dumps(poll))
    assert resp.status_code == status['status']
    assert status['result'] in resp.text


@pytest.mark.parametrize("table, query, status", [
    (
        "test",
        {
            "columns": ["a", "b", "t", "n.a", "n.b", "ip"],
            "data": [[21, "a", "2021-01-01 23:23:00", [30, 31], ["aa", "ab"], "::10.1.1.1"]]

        }, {
            "status": 400,
            "result": '{"code":1010,"error_msg":"None of poll_id in \'poll_ids\' is valid"'
        }
    ),
    (
        "test2",
        {
            "columns": ["i"],
            "data": [[21], [30]]
        }, {
            "status": 200,
            "result": """"progress":"""
        }
    )
])
def test_poll_status_in_batch_case(table, query, status):
    instance.ip_address = "localhost"
    # insert data
    resp = instance.http_request(method="POST", url="dae/v1/ingest/default/tables/" + table, data=json.dumps(query))
    result = json.loads(resp.content)
    assert 'poll_id' in result
    assert 'query_id' in result
    assert 'channel_id' in result
    req = {"channel_id": result['channel_id'], "poll_ids": [result['poll_id']]}
    # send again
    resp = instance.http_request(method="POST", url="dae/v1/ingest/default/tables/" + table, data=json.dumps(query))
    result = json.loads(resp.content)
    # get status
    req['poll_ids'].append(result['poll_id'])
    resp = instance.http_request(method="POST", url="dae/v1/ingest/statuses", data=json.dumps(req))
    assert resp.status_code == status['status']
    result = json.loads(resp.content)
    if resp.status_code == 200:
        assert len(result['status']) == 2
    assert status['result'] in resp.text
