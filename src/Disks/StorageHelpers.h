#pragma once
#include <Interpreters/Context_fwd.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace cluster
{
namespace protocol
{
struct DiskDescriptor;
using DiskDescriptorPtr = std::shared_ptr<DiskDescriptor>;

struct StoragePolicyDescriptor;
using StoragePolicyDescriptorPtr = std::shared_ptr<StoragePolicyDescriptor>;
}
}

namespace DB
{
class ASTCreateDiskQuery;
using StoragePolicyConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

cluster::protocol::DiskDescriptorPtr getDiskDescriptorFromAST(ASTCreateDiskQuery & creat_disk_query, ContextPtr context);

StoragePolicyConfigurationPtr getStoragePolicyConfigurationFromYaml(const std::string & policy_name, const std::string & yaml_str);

/// get and validate StoragePolicyDescriptorPtr from storage configuration, throw Exception if configuration is invalid
cluster::protocol::StoragePolicyDescriptorPtr
getStoragePolicyDescriptorFromConfiguration(const std::string & policy_name, const StoragePolicyConfigurationPtr & config, ContextPtr & context);

StoragePolicyConfigurationPtr getStoragePolicyConfigurationFromDescriptor(const cluster::protocol::StoragePolicyDescriptorPtr & desc);

void createStoragePolicyFromDescriptor(const cluster::protocol::StoragePolicyDescriptorPtr & desc, ContextPtr context);

}
