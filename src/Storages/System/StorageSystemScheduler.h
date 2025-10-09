#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `system.scheduler` table, which allows you to get information about scheduling nodes.
class StorageSystemScheduler final : public shared_ptr_helper<StorageSystemScheduler>, public IStorageSystemOneBlock<StorageSystemScheduler>
{
    friend struct shared_ptr_helper<StorageSystemScheduler>;
public:
    std::string getName() const override { return "SystemScheduler"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
