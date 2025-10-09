#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/// proton: starts system.server table
/// Shows server configuration, ports, and metrics in one row
/// Pure single-instance monitoring for the local server
class StorageSystemServer final : public shared_ptr_helper<StorageSystemServer>, public IStorageSystemOneBlock<StorageSystemServer>
{
public:
    std::string getName() const override { return "SystemServer"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};
/// proton: ends

}
