import logging
import pytest
import os
import time
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NAMED_COLLECTIONS_CONFIG = os.path.join(
    SCRIPT_DIR, "./configs/config.d/named_collections.xml"
)

ZK_PATH = "/named_collections_path"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/named_collections.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_with_keeper",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_with_keeper_2",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_only_named_collection_control",
            main_configs=[
                "configs/config.d/named_collections.xml",
            ],
            user_configs=[
                "configs/users.d/users_only_named_collection_control.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_no_default_access",
            main_configs=[
                "configs/config.d/named_collections.xml",
            ],
            user_configs=[
                "configs/users.d/users_no_default_access.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def replace_in_server_config(node, old, new):
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/named_collections.xml",
        old,
        new,
    )


def test_config_reload(cluster):
    node = cluster.instances["node"]
    assert (
            "collection1" == node.query("select name from system.named_collections").strip()
    )
    assert (
            "['key1']"
            == node.query(
        "select mapKeys(collection) from system.named_collections where name = 'collection1'"
    ).strip()
    )
    assert (
            "value1"
            == node.query(
        "select collection['key1'] from system.named_collections where name = 'collection1'"
    ).strip()
    )

    replace_config(node, "value1", "value2")
    node.query("SYSTEM RELOAD CONFIG")

    assert (
            "['key1']"
            == node.query(
        "select mapKeys(collection) from system.named_collections where name = 'collection1'"
    ).strip()
    )
    assert (
            "value2"
            == node.query(
        "select collection['key1'] from system.named_collections where name = 'collection1'"
    ).strip()
    )


@pytest.mark.parametrize("with_keeper", [False, True])
def test_sql_commands(cluster, with_keeper):
    zk = None
    node = None
    if with_keeper:
        node = cluster.instances["node_with_keeper"]
        zk = cluster.get_kazoo_client("zoo1")
    else:
        node = cluster.instances["node"]

    assert "1" == node.query("select count() from system.named_collections").strip()

    node.query("CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'")

    def check_created():
        assert (
                "collection1\ncollection2"
                == node.query("select name from system.named_collections").strip()
        )

        assert (
                "['key1','key2']"
                == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection2'"
        ).strip()
        )

        assert (
                "1"
                == node.query(
            "select collection['key1'] from system.named_collections where name = 'collection2'"
        ).strip()
        )

        assert (
                "value2"
                == node.query(
            "select collection['key2'] from system.named_collections where name = 'collection2'"
        ).strip()
        )
        if zk is not None:
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 1, key2 = 'value2'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_created()
    node.restart_clickhouse()
    check_created()

    node.query("ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'")

    def check_altered():
        assert (
                "['key1','key2','key3']"
                == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection2'"
        ).strip()
        )

        assert (
                "4"
                == node.query(
            "select collection['key1'] from system.named_collections where name = 'collection2'"
        ).strip()
        )

        assert (
                "value3"
                == node.query(
            "select collection['key3'] from system.named_collections where name = 'collection2'"
        ).strip()
        )

        if zk is not None:
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 4, key2 = 'value2', key3 = 'value3'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_altered()
    node.restart_clickhouse()
    check_altered()

    node.query("ALTER NAMED COLLECTION collection2 DELETE key2")

    def check_deleted():
        assert (
                "['key1','key3']"
                == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection2'"
        ).strip()
        )

        if zk is not None:
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 4, key3 = 'value3'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_deleted()
    node.restart_clickhouse()
    check_deleted()

    node.query(
        "ALTER NAMED COLLECTION collection2 SET key3=3, key4='value4' DELETE key1"
    )
    time.sleep(2)

    def check_altered_and_deleted():
        assert (
                "['key3','key4']"
                == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection2'"
        ).strip()
        )

        assert (
                "3"
                == node.query(
            "select collection['key3'] from system.named_collections where name = 'collection2'"
        ).strip()
        )

        assert (
                "value4"
                == node.query(
            "select collection['key4'] from system.named_collections where name = 'collection2'"
        ).strip()
        )

        if zk is not None:
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key3 = 3, key4 = 'value4'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_altered_and_deleted()
    node.restart_clickhouse()
    check_altered_and_deleted()

    node.query("DROP NAMED COLLECTION collection2")

    def check_dropped():
        assert "1" == node.query("select count() from system.named_collections").strip()
        assert (
                "collection1"
                == node.query("select name from system.named_collections").strip()
        )
        if zk is not None:
            children = zk.get_children(ZK_PATH)
            assert 0 == len(children)

    check_dropped()
    node.restart_clickhouse()
    check_dropped()


def test_keeper_storage(cluster):
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]
    zk = cluster.get_kazoo_client("zoo1")

    assert "1" == node1.query("select count() from system.named_collections").strip()
    assert "1" == node2.query("select count() from system.named_collections").strip()

    node1.query("CREATE NAMED COLLECTION collection2 AS key1=1, key2='value2'")

    def check_created(node):
        assert (
            "collection1\ncollection2"
            == node.query("select name from system.named_collections").strip()
        )

        assert (
            "['key1','key2']"
            == node.query(
                "select mapKeys(collection) from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "1"
            == node.query(
                "select collection['key1'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "value2"
            == node.query(
                "select collection['key2'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

        children = zk.get_children(ZK_PATH)
        assert 1 == len(children)
        assert "collection2.sql" in children
        assert (
            b"CREATE NAMED COLLECTION collection2 AS key1 = 1, key2 = 'value2'"
            in zk.get(ZK_PATH + "/collection2.sql")[0]
        )

    check_created(node1)
    check_created(node2)

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    check_created(node1)
    check_created(node2)

    node2.query("ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'")

    time.sleep(5)

    def check_altered(node):
        assert (
            "['key1','key2','key3']"
            == node.query(
                "select mapKeys(collection) from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "4"
            == node.query(
                "select collection['key1'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

        assert (
            "value3"
            == node.query(
                "select collection['key3'] from system.named_collections where name = 'collection2'"
            ).strip()
        )

        if zk is not None:
            children = zk.get_children(ZK_PATH)
            assert 1 == len(children)
            assert "collection2.sql" in children
            assert (
                b"CREATE NAMED COLLECTION collection2 AS key1 = 4, key2 = 'value2', key3 = 'value3'"
                in zk.get(ZK_PATH + "/collection2.sql")[0]
            )

    check_altered(node2)
    check_altered(node1)

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    check_altered(node1)
    check_altered(node2)

    node1.query("DROP NAMED COLLECTION collection2")

    time.sleep(5)

    def check_dropped(node):
        assert "1" == node.query("select count() from system.named_collections").strip()
        assert (
            "collection1"
            == node.query("select name from system.named_collections").strip()
        )
        if zk is not None:
            children = zk.get_children(ZK_PATH)
            assert 0 == len(children)

    check_dropped(node1)
    check_dropped(node2)

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    check_dropped(node1)
    check_dropped(node2)
