#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <base/shared_ptr_helper.h>

namespace DB
{

/** Implements `inputs` system table.
  *
  * Inputs are user-facing aliases for externally-ingested streams created via `CREATE INPUT ...`.
  * The authoritative metadata (created/modified info and stored DDL) comes from the MetaStore.
  */
class StorageSystemInputs final : public shared_ptr_helper<StorageSystemInputs>, public IStorageSystemOneBlock<StorageSystemInputs>
{
    friend struct shared_ptr_helper<StorageSystemInputs>;

public:
    std::string getName() const override { return "SystemInputs"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
