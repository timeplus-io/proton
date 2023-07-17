#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Storages/Cache/SchemaCache.h>

namespace DB
{

class StorageSystemSchemaInferenceCache final : public shared_ptr_helper<StorageSystemSchemaInferenceCache>, public IStorageSystemOneBlock<StorageSystemSchemaInferenceCache>
{
public:
    std::string getName() const override { return "SystemSettingsChanges"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
