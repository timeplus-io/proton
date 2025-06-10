#include "ADBCDriver.h"
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
    ADBCDriver() {
        // Initialize the ADBC driver manager
        AdbcStatusCode status = AdbcDriverManagerInitialize();
        if (status != ADBC_STATUS_OK) {
            std::cerr << "Failed to initialize ADBC driver manager: " << AdbcStatusCodeToString(status) << std::endl;
            throw std::runtime_error("Failed to initialize ADBC driver manager");
        }

        // Register the Arrow Flight SQL driver
        status = AdbcDriverManagerRegisterDriver("arrow-flight-sql", &arrow_flight_sql_driver);
        if (status != ADBC_STATUS_OK) {
            std::cerr << "Failed to register Arrow Flight SQL driver: " << AdbcStatusCodeToString(status) << std::endl;
            throw std::runtime_error("Failed to register Arrow Flight SQL driver");
        }
    }

    ~ADBCDriver() {
        // Cleanup the ADBC driver manager
        AdbcDriverManagerCleanup();
    }

    std::shared_ptr<arrow::Table> ExecuteQuery(const std::string& query) {
        // Create a connection to the Arrow Flight SQL server
        AdbcConnection connection;
        AdbcStatusCode status = AdbcConnectionInitialize(&connection, "arrow-flight-sql", nullptr);
        if (status != ADBC_STATUS_OK) {
            std::cerr << "Failed to initialize ADBC connection: " << AdbcStatusCodeToString(status) << std::endl;
            throw std::runtime_error("Failed to initialize ADBC connection");
        }

        // Execute the query
        AdbcStatement statement;
        status = AdbcStatementInitialize(&statement, &connection);
        if (status != ADBC_STATUS_OK) {
            std::cerr << "Failed to initialize ADBC statement: " << AdbcStatusCodeToString(status) << std::endl;
            throw std::runtime_error("Failed to initialize ADBC statement");
        }

        status = AdbcStatementSetSqlQuery(&statement, query.c_str());
        if (status != ADBC_STATUS_OK) {
            std::cerr << "Failed to set SQL query: " << AdbcStatusCodeToString(status) << std::endl;
            throw std::runtime_error("Failed to set SQL query");
        }

        AdbcResult result;
        status = AdbcStatementExecuteQuery(&statement, &result);
        if (status != ADBC_STATUS_OK) {
            std::cerr << "Failed to execute query: " << AdbcStatusCodeToString(status) << std::endl;
            throw std::runtime_error("Failed to execute query");
        }

        // Convert the result to an Arrow Table
        std::shared_ptr<arrow::Table> table;
        arrow::Status arrow_status = arrow::flight::sql::ResultToTable(result, &table);
        if (!arrow_status.ok()) {
            std::cerr << "Failed to convert result to Arrow Table: " << arrow_status.ToString() << std::endl;
            throw std::runtime_error("Failed to convert result to Arrow Table");
        }

        // Cleanup
        AdbcResultRelease(&result);
        AdbcStatementRelease(&statement);
        AdbcConnectionRelease(&connection);

        return table;
    }

private:
    AdbcDriver arrow_flight_sql_driver;
};

} // namespace ADBC
