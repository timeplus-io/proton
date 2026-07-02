#include <Storages/System/StorageSystemInputs.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListStreamsRequest.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/InputTargetStreams.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/StringUtils/StringUtils.h>

#include <Interpreters/InputSettingsUtils.h>

namespace DB
{

NamesAndTypesList StorageSystemInputs::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"type", std::make_shared<DataTypeString>()},
        {"target_streams", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"created", std::make_shared<DataTypeDateTime64>(3, "UTC")},
        {"last_modified_by", std::make_shared<DataTypeString>()},
        {"created_by", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemInputs::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    /// Single-instance: read stream descriptors directly from the local meta-store
    /// instead of routing the request through the cluster app meta client.
    auto & metastore = Globals::getMetaStore();
    auto resp = metastore.listStreams(std::make_shared<cluster::ListStreamsRequest>(
        "", "", metastore.nodeID(), /*consistent_read_=*/false, /*timeout_ms_=*/10'000, /*request_version=*/2));
    if (resp->hasError())
    {
        const auto & err = resp->error();
        throw Exception(err.error_code, "Failed to load streams: {}", err.error_message);
    }

    const auto & stream_descs = resp->data().stream_descs;
    for (auto & col : res_columns)
        col->reserve(stream_descs.size());

    for (const auto & desc_ptr : stream_descs)
    {
        if (!desc_ptr)
            continue;

        const auto & desc = *desc_ptr;
        if (desc.sql_def.find("INPUT") == std::string::npos)
            continue;

        ASTPtr ast;
        try
        {
            ast = DB::parseQuery(desc.sql_def, context);
        }
        catch (...)
        {
            continue;
        }

        const auto * create = ast->as<ASTCreateQuery>();
        if (!create || !create->is_input)
            continue;

        String type;
        Array targets;
        if (create->storage && create->storage->settings)
        {
            if (const Field * type_field = create->storage->settings->changes.tryGet("type");
                type_field && type_field->getType() == Field::Types::String)
            {
                type = type_field->safeGet<String>();
            }

            auto target_streams = extractTargetStreamValues(create->storage->settings);
            targets.reserve(target_streams.size());
            for (const auto & target : target_streams)
                targets.emplace_back(target);
        }

        if (targets.empty())
        {
            auto runtime_targets = tryGetInputTargetStreamsFromRuntime(desc.stream.ns, desc.stream.name, context);
            targets.reserve(runtime_targets.size());
            for (const auto & target : runtime_targets)
                targets.emplace_back(target);
        }

        size_t col_idx = 0;
        res_columns[col_idx++]->insert(desc.stream.ns);
        res_columns[col_idx++]->insert(desc.stream.name);
        res_columns[col_idx++]->insert(desc.stream.id);
        res_columns[col_idx++]->insert(type);
        res_columns[col_idx++]->insert(targets);
        res_columns[col_idx++]->insert(DateTime64(desc.create_timestamp_ms));
        res_columns[col_idx++]->insert(desc.last_modified_by);
        res_columns[col_idx++]->insert(desc.created_by);
    }
}

}
