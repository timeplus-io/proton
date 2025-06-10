#pragma once

#include <adbc.h>
#include <adbc_driver_manager.h>
#include <arrow/flight/sql/client.h>
#include <arrow/flight/sql/driver.h>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/sql/types.h>
#include <arrow/flight/sql/utils.h>
#include <arrow/flight/types.h>
#include <arrow/flight/utils.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/logging.h>
#include <arrow/util/memory.h>
#include <arrow/util/uri.h>
#include <iostream>
#include <memory>
#include <string>

namespace ADBC {

class ADBCDriver {
public:
    ADBCDriver();
    ~ADBCDriver();

    std::shared_ptr<arrow::Table> ExecuteQuery(const std::string& query);

private:
    AdbcDriver arrow_flight_sql_driver;
};

} // namespace ADBC
