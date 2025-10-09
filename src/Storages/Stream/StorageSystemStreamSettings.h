#pragma once

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <base/shared_ptr_helper.h>


namespace DB
{

class Context;

/** implements system table "stream_settings",
  *  which allows to get information about the current Stream settings.
  */
class SystemStreamSettings final : public shared_ptr_helper<SystemStreamSettings>, public IStorageSystemOneBlock<SystemStreamSettings>
{
    friend struct shared_ptr_helper<SystemStreamSettings>;

public:
    std::string getName() const override { return "SystemStreamSettings"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock<SystemStreamSettings>::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
