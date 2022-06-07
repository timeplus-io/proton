#pragma once

#include <Coordination/MetaStoreDispatcher.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <base/types.h>
#include <Common/config.h>
#include "config_core.h"

#include <mutex>
#include <unordered_set>
#include <Poco/Timestamp.h>


namespace DB
{
/// JSON config repository via MetaStore used by ExternalLoader.
class MetaStoreJSONConfigRepository : public IExternalLoaderConfigRepository
{
public:
    MetaStoreJSONConfigRepository(const std::shared_ptr<MetaStoreDispatcher> & metastore_dispatcher_, const std::string & namespace_);

    std::string getName() const override { return name; }

    /// Return set of function names from metastore
    std::set<std::string> getAllLoadablesDefinitionNames() override;

    /// Checks that file with name exists on metastore
    bool exists(const std::string & definition_entity_name) override;

    Poco::Timestamp getUpdateTime(const std::string & definition_entity_name) override;

    LoadablesConfigurationPtr load(const std::string & key) override;

    Poco::JSON::Object::Ptr get(const std::string & key) const;

    Poco::JSON::Object::Ptr readConfigKey(const std::string & key) const;

    void save(const std::string & key, const Poco::JSON::Object::Ptr & config);

    void remove(const std::string & config_file);

    Poco::JSON::Array::Ptr list() const;

private:
    std::shared_ptr<MetaStoreDispatcher> metastore_dispatcher;

    const std::string name;

    const std::string ns;

    const std::string main_config_path;
};

}
