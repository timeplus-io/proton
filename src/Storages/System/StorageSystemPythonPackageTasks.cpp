#include <Storages/System/StorageSystemPythonPackageTasks.h>

#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>

#if USE_PYTHON_UDF
#include <CPython/AsyncPythonPackageManager.h>
#endif

namespace DB
{

NamesAndTypesList StorageSystemPythonPackageTasks::getNamesAndTypes()
{
    return {
        {"task_id", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeString>()},
        {"package_name", std::make_shared<DataTypeString>()},
        {"installed_version", std::make_shared<DataTypeString>()},
        {"operation", std::make_shared<DataTypeString>()},
        {"error_code", std::make_shared<DataTypeInt32>()},
        {"error_message", std::make_shared<DataTypeString>()},
        {"created_at", std::make_shared<DataTypeDateTime64>(3)},
        {"started_at", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3))},
        {"completed_at", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3))},
    };
}

void StorageSystemPythonPackageTasks::fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, [[maybe_unused]] const SelectQueryInfo &) const
{
#if USE_PYTHON_UDF
    auto async_manager = context->getAsyncPythonPackageManager();
    if (!async_manager)
        return;

    auto tasks = async_manager->getAllTaskResults();

    for (const auto & task : tasks)
    {
        size_t col_num = 0;

        res_columns[col_num++]->insert(task->task_id);

        res_columns[col_num++]->insert(task->statusString());

        res_columns[col_num++]->insert(task->package_name);
        res_columns[col_num++]->insert(task->installed_version);
        res_columns[col_num++]->insert(task->operation);
        res_columns[col_num++]->insert(task->error.error_code);

        res_columns[col_num++]->insert(task->error.error_message);
        auto to_datetime64_field = [](const std::chrono::system_clock::time_point & tp) -> Field {
            if (tp == std::chrono::system_clock::time_point{})
                return Null();

            auto milliseconds_count = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
            return DateTime64(milliseconds_count);
        };

        auto to_datetime64_required = [](const std::chrono::system_clock::time_point & tp) -> DateTime64 {
            auto milliseconds_count = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
            return DateTime64(milliseconds_count);
        };

        res_columns[col_num++]->insert(to_datetime64_required(task->created_at));
        res_columns[col_num++]->insert(to_datetime64_field(task->started_at));
        res_columns[col_num++]->insert(to_datetime64_field(task->completed_at));
    }
#endif
}

}
