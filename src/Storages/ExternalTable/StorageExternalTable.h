#pragma once

#include "Interpreters/Context_fwd.h"
#include "Storages/ExternalTable/ExternalTableSettings.h"
#include "Storages/IStorage.h"
#include "base/shared_ptr_helper.h"

namespace DB
{

class StorageExternalTable final : public shared_ptr_helper<StorageExternalTable>, public IStorage, public WithContext
{
    friend struct shared_ptr_helper<StorageExternalTable>;

public:
    String getName() const override { return "ExternalTable"; }

    bool isRemote() const override { return true; }
    bool isExternalTable() const override { return true; }

    /// FIXME
    void startup() override { std::cout << "ExternalTable startup" << std::endl; }
    /// FIXME
    void shutdown() override { std::cout << "ExternalTable shutdown" << std::endl; }

protected:
    StorageExternalTable(
        const StorageID & table_id_,
        std::unique_ptr<ExternalTableSettings> external_table_settings_,
        ContextPtr context_);
};

}
