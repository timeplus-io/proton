#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_STREAM;
extern const int STREAM_IS_DROPPED;
extern const int RESOURCE_NOT_FOUND;
}
}

namespace DB::V8
{

void CacheDictionaryBridge::InsertToBackendStorage(Block && block, const DB::StorageID & table_id)
{
    try
    {
        auto insert = std::make_unique<DB::ASTInsertQuery>();
        insert->table_id = table_id;
        DB::ASTPtr query_ptr = DB::ASTPtr(insert.release());

        auto insert_context = DB::Context::createCopy(this->global_context);
        insert_context->makeQueryContext();
        DB::InterpreterInsertQuery interpreter(query_ptr, insert_context);
        DB::BlockIO io = interpreter.execute();

        DB::PushingPipelineExecutor executor(io.pipeline);
        executor.start();
        executor.push(std::move(block));
        executor.finish();

        LOG_TRACE(logger, "Insert to backend storage successful for table {}.{}", table_id.getDatabaseName(), table_id.getTableName());
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Error in InsertToBackendStorage: {}", e.displayText());
        /// Invalidate cache for known stream missing/dropped cases
        if (e.code() == ErrorCodes::UNKNOWN_STREAM || e.code() == ErrorCodes::STREAM_IS_DROPPED
            || e.code() == ErrorCodes::RESOURCE_NOT_FOUND)
        {
            std::unique_lock lock(dict_map_rw_mutex);
            for (auto it = cached_source_db_and_table.begin(); it != cached_source_db_and_table.end(); ++it)
            {
                if (it->second == table_id)
                {
                    const std::string dict_name = it->first;
                    LOG_WARNING(logger, "Source missing (code={}), clearing cache for dictionary '{}'", e.code(), dict_name);
                    dictionaries.erase(dict_name);
                    cached_sample_blocks.erase(dict_name);
                    cached_source_db_and_table.erase(it);
                    is_simple_key_dict.erase(dict_name);
                    key_managers.erase(dict_name);
                    value_converters.erase(dict_name);
                    break;
                }
            }
        }
        throw;
    }
    catch (...)
    {
        LOG_ERROR(logger, "Unknown error in InsertToBackendStorage");
        throw;
    }
}
}
