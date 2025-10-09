#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <base/shared_ptr_helper.h>


namespace DB
{

class Context;

/// Implements `system.python_package_tasks` table, which provides information about async Python package operations.
class StorageSystemPythonPackageTasks final : public shared_ptr_helper<StorageSystemPythonPackageTasks>,
                                              public IStorageSystemOneBlock<StorageSystemPythonPackageTasks>
{
public:
    std::string getName() const override { return "SystemPythonPackageTasks"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
