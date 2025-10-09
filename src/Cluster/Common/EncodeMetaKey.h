#pragma once

#include <string>

#include <Cluster/Common/MetaKeySpace.h>
#include <Cluster/Common/NodeID.h>

namespace cluster
{
struct StreamIDShard;

MetaKeySpace decodeMetaKeySpace(std::string_view key);
std::string encodeMetaStreamKey();
std::string encodeMetaStreamKey(const std::string & ns);
std::string encodeMetaStreamKey(const std::string & ns, const std::string & name);

std::string encodeMetaUDFKey();
std::string encodeMetaUDFKey(const std::string & name);

std::string encodeMetaFormatSchemaKey();
std::string encodeMetaFormatSchemaKey(const std::string & name, const std::string & format);

std::string encodeMetaAccessEntityKey();
std::string encodeMetaAccessEntityKey(const std::string & id);

std::string encodeMetaNodeKey();
std::string encodeMetaNodeKey(NodeID node_id);

std::string encodeMetaDatabaseKey();
std::string encodeMetaDatabaseKey(const std::string & name);

std::string encodeMetaDiskKey();
std::string encodeMetaDiskKey(const std::string & name);

std::string encodeMetaStoragePolicyKey();
std::string encodeMetaStoragePolicyKey(const std::string & name);

std::string encodeMetaMaterializedViewAssignmentKey();
std::string encodeMetaMaterializedViewAssignmentKey(const std::string & ns, const std::string & name);

std::string encodeMetaAlertKey();
std::string encodeMetaAlertKey(const std::string & ns);
std::string encodeMetaAlertKey(const std::string & ns, const std::string & name);

std::string encodeMetaTaskKey();
std::string encodeMetaTaskKey(const std::string & ns);
std::string encodeMetaTaskKey(const std::string & ns, const std::string & name);

std::string encodeMetaAppliedSequenceKey(const StreamIDShard & stream_shard);

std::string encodeMetaPendingRequestKey(uint64_t sequence_number);

}
