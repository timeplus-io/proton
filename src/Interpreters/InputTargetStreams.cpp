#include <Interpreters/InputTargetStreams.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>

#include <algorithm>

namespace DB
{

std::vector<String> tryGetInputTargetStreamsFromRuntime(const String & database, const String & input, ContextPtr context)
{
    if (!context || database.empty() || input.empty())
        return {};

    try
    {
        const StorageID input_id{database, input};
        auto storage = DatabaseCatalog::instance().tryGetTable(input_id, context);
        if (!storage)
            return {};

        const auto * external_stream = dynamic_cast<const StorageExternalStream *>(storage.get());
        if (!external_stream)
            return {};

        auto nested = std::dynamic_pointer_cast<const StorageExternalStreamImpl>(external_stream->getNested());
        if (!nested)
            return {};

        std::vector<String> targets;
        for (const auto & target : nested->getTargetTables())
        {
            if (target.database_name.empty())
            {
                targets.push_back(target.table_name);
                continue;
            }

            if (target.database_name == database)
                targets.push_back(target.table_name);
            else
                targets.push_back(target.getFullNameNotQuoted());
        }

        std::sort(targets.begin(), targets.end());
        targets.erase(std::unique(targets.begin(), targets.end()), targets.end());
        return targets;
    }
    catch (...)
    {
        return {};
    }
}

}
