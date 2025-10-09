#include "config.h"

#if USE_YAML_CPP
#include "gtest_helper_functions.h"
#include <base/scope_guard.h>
#include <Common/Config/YAMLParser.h>
#include <Common/Config/ConfigHelper.h>
#include <Poco/AutoPtr.h>
#include "Poco/DOM/Document.h"

#include <gtest/gtest.h>


using namespace DB;

TEST(YamlParser, InvalidFile)
{
    ASSERT_THROW(YAMLParser::parse("some-non-existing-file.yaml"), Exception);
}

TEST(YamlParser, ProcessValuesList)
{
    auto yaml_file = getFileWithContents("values-list.yaml", R"YAML(
rules:
  - apiGroups: [""]
    resources:
      - nodes
      - nodes/proxy
      - services
      - endpoints
      - pods
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/proton");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<proton>
<rules>
<apiGroups></apiGroups>
<resources>nodes</resources>
<resources>nodes/proxy</resources>
<resources>services</resources>
<resources>endpoints</resources>
<resources>pods</resources>
</rules>
</proton>
)CONFIG");

}

TEST(YamlParser, ProcessKeysList)
{
    auto yaml_file = getFileWithContents("keys-list.yaml", R"YAML(
operator:
    access_management: 1
    networks:
        ip:
          - 10.1.6.168
          - ::1
          - 127.0.0.1
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/proton");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<proton>
<operator>
<access_management>1</access_management>
<networks>
<ip>10.1.6.168</ip>
<ip>::1</ip>
<ip>127.0.0.1</ip>
</networks>
</operator>
</proton>
)CONFIG");

}

TEST(YamlParser, ProcessListAttributes)
{
    auto yaml_file = getFileWithContents("list_attributes.yaml", R"YAML(
seq:
  - "@attr1": x
  - k1: val1
    k2: val2
    "@attr2": y
  - k3: val3
    "@attr3": z
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/proton");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<proton>
<seq attr1="x"></seq>
<seq attr2="y">
<k1>val1</k1>
<k2>val2</k2>
</seq>
<seq attr3="z">
<k3>val3</k3>
</seq>
</proton>
)CONFIG");

}

TEST(YamlParser, ProcessMapAttributes)
{
    auto yaml_file = getFileWithContents("map_attributes.yaml", R"YAML(
map:
    "@attr1": x
    k1: val1
    k2: val2
    "@attr2": y
    k3: val3
    "@attr3": z
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/proton");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<proton>
<map attr1="x" attr2="y" attr3="z">
<k1>val1</k1>
<k2>val2</k2>
<k3>val3</k3>
</map>
</proton>
)CONFIG");

}

TEST(YamlParser, ClusterDef)
{
    auto yaml_file = getFileWithContents("cluster_def.yaml", R"YAML(
test_cluster:
    shard:
        - internal_replication: false
          replica:
              - host: 127.0.0.1
                port: 9000
              - host: 127.0.0.2
                port: 9000
        - internal_replication: true
          replica:
              - host: 127.0.0.3
                port: 9000
              - host: 127.0.0.4
                port: 9000
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/proton");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<proton>
<test_cluster>
<shard>
<internal_replication>false</internal_replication>
<replica>
<host>127.0.0.1</host>
<port>9000</port>
</replica>
<replica>
<host>127.0.0.2</host>
<port>9000</port>
</replica>
</shard>
<shard>
<internal_replication>true</internal_replication>
<replica>
<host>127.0.0.3</host>
<port>9000</port>
</replica>
<replica>
<host>127.0.0.4</host>
<port>9000</port>
</replica>
</shard>
</test_cluster>
</proton>
)CONFIG");

}


TEST(YamlParser, RaftConfiguration)
{
    auto yaml_file = getFileWithContents("raft_configuration.yaml", R"YAML(
raft_configuration:
    server:
        - id: 1
          hostname: timeplus-server1
          port: 9445
          http_port: 3218
        - id: 2
          hostname: timeplus-server2
          port: 9445
          http_port: 3218
        - id: 3
          hostname: timeplus-server3
          port: 9445
          http_port: 3218
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/proton");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<proton>
<raft_configuration>
<server>
<id>1</id>
<hostname>timeplus-server1</hostname>
<port>9445</port>
<http_port>3218</http_port>
</server>
<server>
<id>2</id>
<hostname>timeplus-server2</hostname>
<port>9445</port>
<http_port>3218</http_port>
</server>
<server>
<id>3</id>
<hostname>timeplus-server3</hostname>
<port>9445</port>
<http_port>3218</http_port>
</server>
</raft_configuration>
</proton>
)CONFIG");

}

TEST(YamlParser, StoragePolicy)
{
    Poco::AutoPtr<Poco::XML::Document> xml =  YAMLParser::parseContent(R"YAML(
    volumes:
        main:
            disk: default
        s3:
            disk: s3
    move_factor: 0.8
)YAML");
    auto *p_node = xml->getNodeByPath("/proton");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<proton>
<volumes>
<main>
<disk>default</disk>
</main>
<s3>
<disk>s3</disk>
</s3>
</volumes>
<move_factor>0.8</move_factor>
</proton>
)CONFIG");

}


#endif
