#include "DDLService.h"

#include "CatalogService.h"
#include "PlacementService.h"
#include "TaskStatusService.h"

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <IO/HTTPCommon.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNRETRIABLE_ERROR;
}

namespace
{
    /// globals
    const String DDL_KEY_PREFIX = "cluster_settings.system_ddls.";
    const String DDL_NAME_KEY = DDL_KEY_PREFIX + "name";
    const String DDL_REPLICATION_FACTOR_KEY = DDL_KEY_PREFIX + "replication_factor";
    const String DDL_DATA_RETENTION_KEY = DDL_KEY_PREFIX + "data_retention";
    const String DDL_DEFAULT_TOPIC = "__system_ddls";

    const String DDL_TABLE_PATCH_API_PATH_FMT = "/dae/v1/ddl/{}/{}/{}";
    const String DDL_TABLE_POST_API_PATH_FMT = "/dae/v1/ddl/{}/{}";
    const String DDL_DATABSE_POST_API_PATH_FMT = "/dae/v1/ddl/databases";
    const String DDL_DATABSE_DELETE_API_PATH_FMT = "/dae/v1/ddl/databases/{}";

    int toErrorCode(std::istream & istr)
    {
        std::stringstream error_message; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        error_message << istr.rdbuf();
        /// FIXME, revise HTTP status code for the legacy HTTP API
        const auto & msg = error_message.str();
        if (msg.find("Code: 57") != std::string_view::npos)
        {
            /// Table already exists. FIXME: enhance table creation idempotent by adding query id
            return ErrorCodes::UNRETRIABLE_ERROR;
        }
        /// FIXME, other un-retriable error codes
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }

    int toErrorCode(const Poco::Exception & e)
    {
        /// FIXME, more error code handling
        auto code = e.code();
        if (code == 1000 || code == 422)
        {
            return ErrorCodes::UNRETRIABLE_ERROR;
        }
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }

    String getURIEndpoint(const std::unordered_map<String, String> & headers)
    {
        if (headers.contains("table_type") && headers.at("table_type") == "rawstore")
        {
            return "rawstores";
        }
        return "tables";
    }

    std::vector<Poco::URI> toURIs(const std::vector<String> & hosts, const String & path, const String & default_port)
    {
        std::vector<Poco::URI> uris;
        uris.reserve(hosts.size());

        for (auto host : hosts)
        {
            /// FIXME : HTTP for now
            if (host.rfind(":") != String::npos)
            {
                /// `host` contains port information
                uris.emplace_back("http://" + host + path);
            }
            else
            {
                uris.emplace_back("http://" + host + default_port + path);
            }
        }

        return uris;
    }

}

DDLService & DDLService::instance(const ContextPtr & global_context_)
{
    static DDLService ddl_service{global_context_};
    return ddl_service;
}

DDLService::DDLService(const ContextPtr & global_context_)
    : MetadataService(global_context_, "DDLService")
    , http_port(":" + global_context_->getConfigRef().getString("http_port"))
    , catalog(CatalogService::instance(global_context_))
    , placement(PlacementService::instance(global_context_))
    , task(TaskStatusService::instance(global_context_))
{
}

MetadataService::ConfigSettings DDLService::configSettings() const
{
    return {
        .name_key = DDL_NAME_KEY,
        .default_name = DDL_DEFAULT_TOPIC,
        .data_retention_key = DDL_DATA_RETENTION_KEY,
        .default_data_retention = 168,
        .replication_factor_key = DDL_REPLICATION_FACTOR_KEY,
        .request_required_acks = -1,
        .request_timeout_ms = 10000,
        .auto_offset_reset = "earliest",
        .initial_default_offset = -1000,
    };
}

inline void DDLService::updateDDLStatus(
    const String & query_id,
    const String & user,
    const String & status,
    const String & query,
    const String & progress,
    const String & reason) const
{
    auto task_status = std::make_shared<TaskStatusService::TaskStatus>();
    task_status->id = query_id;
    task_status->user = user;
    task_status->status = status;

    task_status->context = query;
    task_status->progress = progress;
    task_status->reason = reason;

    task.append(task_status);
}

void DDLService::progressDDL(const String & query_id, const String & user, const String & query, const String & progress) const
{
    updateDDLStatus(query_id, user, TaskStatusService::TaskStatus::INPROGRESS, query, progress, "");
}

void DDLService::succeedDDL(const String & query_id, const String & user, const String & query) const
{
    updateDDLStatus(query_id, user, TaskStatusService::TaskStatus::SUCCEEDED, query, "", "");
}

void DDLService::failDDL(const String & query_id, const String & user, const String & query, const String reason) const
{
    updateDDLStatus(query_id, user, TaskStatusService::TaskStatus::FAILED, query, "", reason);
}

bool DDLService::validateSchema(const Block & block, const std::vector<String> & col_names) const
{
    for (const auto & col_name : col_names)
    {
        if (!block.has(col_name))
        {
            LOG_ERROR(log, "`{}` column is missing", col_name);

            String query_id = block.getByName("query_id").column->getDataAt(0).toString();
            String user = "";
            if (block.has("user"))
            {
                user = block.getByName("user").column->getDataAt(0).toString();
            }
            failDDL(query_id, user, "", "invalid DDL");
            return false;
        }
    }
    return true;
}

Int32 DDLService::sendRequest(
    const String & payload, const Poco::URI & uri, const String & method, const String & query_id) const
{
    /// One second for connect/send/receive
    ConnectionTimeouts timeouts({1, 0}, {1, 0}, {5, 0});

    PooledHTTPSessionPtr session;
    try
    {
        session = makePooledHTTPSession(uri, timeouts, 1);
        Poco::Net::HTTPRequest request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        request.setHost(uri.getHost());
        request.setContentLength(payload.size());
        request.setContentType("application/json");
        request.add("X-ClickHouse-Query-Id", query_id);

        auto & ostr = session->sendRequest(request);
        ostr << payload;

        if (!ostr.good())
        {
            LOG_ERROR(log, "Failed to send request data {} to uri={}", payload, uri.toString());
            return ErrorCodes::UNKNOWN_EXCEPTION;
        }

        Poco::Net::HTTPResponse response;
        /// FIXME: handle table exists, table not exists etc error explicitly
        auto & istr = session->receiveResponse(response);
        auto status = response.getStatus();
        if (status == Poco::Net::HTTPResponse::HTTP_OK)
        {
            LOG_INFO(
                log, "Executed DDL operation on uri={} successfully, method={}, payload={}", uri.toString(), method, payload);
            return ErrorCodes::OK;
        }
        else
        {
            return toErrorCode(istr);
        }
    }
    catch (const Poco::Exception & e)
    {
        if (!session.isNull())
        {
            session->attachSessionData(e.message());
        }

        LOG_ERROR(
            log,
            "Failed to do DDL operation on uri={} method={} error={} exception={}",
            uri.toString(),
            method,
            e.message(),
            getCurrentExceptionMessage(true, true));

        return toErrorCode(e);
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to do DDL operation on uri={} method={} exception={}",
            uri.toString(),
            method,
            getCurrentExceptionMessage(true, true));
    }
    return ErrorCodes::UNKNOWN_EXCEPTION;
}

/// Try indefininitely
Int32 DDLService::doDDL(const String & payload, const Poco::URI & uri, const String & method, const String & query_id) const
{
    /// FIXME, retry several times and report error
    Int32 err = ErrorCodes::OK;

    while (!stopped.test())
    {
        try
        {
            err = sendRequest(payload, uri, method, query_id);
            if (err == ErrorCodes::OK || err == ErrorCodes::UNRETRIABLE_ERROR)
            {
                return err;
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to send request to uri={} exception={}", uri.toString(), getCurrentExceptionMessage(true, true));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    return err;
}

void DDLService::createTable(IDistributedWriteAheadLog::RecordPtr record)
{
    Block & block = record->block;
    assert(block.has("query_id"));
    if (!validateSchema(block, {"payload", "database", "table", "shards", "replication_factor", "query_id", "user", "timestamp"}))
    {
        return;
    }

    String database = block.getByName("database").column->getDataAt(0).toString();
    String query_id = block.getByName("query_id").column->getDataAt(0).toString();
    String user = block.getByName("user").column->getDataAt(0).toString();
    String payload = block.getByName("payload").column->getDataAt(0).toString();
    String table = block.getByName("table").column->getDataAt(0).toString();
    Int32 shards = block.getByName("shards").column->getInt(0);
    Int32 replication_factor = block.getByName("replication_factor").column->getInt(0);

    /// FIXME : check with catalog to see if this DDL is fufilled
    /// Build a data structure to cached last 10000 DDLs, check aginst this data structure

    /// Create a DWAL for this table. FIXME: retention_ms
    std::any ctx{DistributedWriteAheadLogKafkaContext{database + "." + table, shards, replication_factor}};
    doCreateDWal(ctx);

    if (block.has("hosts"))
    {
        /// If `hosts` exists in the block, we already placed the replicas
        /// then we move to the execution stage

        String hosts_val = block.getByName("hosts").column->getDataAt(0).toString();
        std::vector<String> hosts;
        boost::algorithm::split(hosts, hosts_val, boost::is_any_of(","));
        assert(!hosts.empty());

        std::vector<Poco::URI> target_hosts{
            toURIs(hosts, fmt::format(DDL_TABLE_POST_API_PATH_FMT, database, getURIEndpoint(record->headers)), http_port)};

        /// Create table on each target host accordign to placement
        for (Int32 i = 0; i < replication_factor; ++i)
        {
            for (Int32 j = 0; j < shards; ++j)
            {
                /// FIXME, check table engine, grammar check
                target_hosts[i * replication_factor + j].setQueryParameters(
                    Poco::URI::QueryParameters{{"distributed_ddl", "false"}, {"shard", std::to_string(j)}});
                auto err = doDDL(payload, target_hosts[i * replication_factor + j], Poco::Net::HTTPRequest::HTTP_POST, query_id);
                if (err == ErrorCodes::UNRETRIABLE_ERROR)
                {
                    failDDL(query_id, user, payload, "Unable to fulfill the request due to unrecoverable failure");
                    return;
                }
            }
        }

        succeedDDL(query_id, user, payload);
    }
    else
    {
        /// Ask placement service to do shard placement
        const auto & qualified_nodes = placement.place(shards, replication_factor);
        if (qualified_nodes.empty())
        {
            LOG_ERROR(
                log,
                "Failed to create table because there are not enough hosts to place its total={} shard replicas, payload={} "
                "query_id={} user={}",
                shards * replication_factor,
                payload,
                query_id,
                user);
            failDDL(query_id, user, payload, "There are not enough hosts to place the table shard replicas");
            return;
        }

        std::vector<String> target_hosts;
        target_hosts.reserve(qualified_nodes.size());
        for (const auto & node : qualified_nodes)
        {
            target_hosts.push_back(node->host + ":" + node->http_port);
        }

        /// We got the placement, commit the placement decision
        /// Add `hosts` column to block
        /// FIXME: put the hosts in header
        auto string_type = DataTypeFactory::instance().get(getTypeName(TypeIndex::String));
        auto host_col = string_type->createColumn();
        String hosts{boost::algorithm::join(target_hosts, ",")};
        host_col->insertData(hosts.data(), hosts.size());
        ColumnWithTypeAndName host_col_with_type(std::move(host_col), string_type, "hosts");
        block.insert(host_col_with_type);

        /// FIXME, retries, error handling
        auto result = dwal->append(*record.get(), dwal_append_ctx);
        if (result.err != ErrorCodes::OK)
        {
            LOG_ERROR(log, "Failed to commit placement decision for create table payload={}", payload);
            failDDL(query_id, user, payload, "Internal server error");
        }
        else
        {
            LOG_INFO(log, "Successfully find placement for create table payload={} placement={}", payload, hosts);
            progressDDL(query_id, user, payload, "shard replicas placed");
        }
    }
}

void DDLService::mutateTable(IDistributedWriteAheadLog::RecordPtr record, const String & method) const
{
    Block & block = record->block;
    assert(block.has("query_id"));

    if (!validateSchema(block, {"payload", "database", "table", "timestamp", "query_id", "user"}))
    {
        return;
    }

    String database = block.getByName("database").column->getDataAt(0).toString();
    String table = block.getByName("table").column->getDataAt(0).toString();
    String query_id = block.getByName("query_id").column->getDataAt(0).toString();
    String user = block.getByName("user").column->getDataAt(0).toString();
    String payload = block.getByName("payload").column->getDataAt(0).toString();

    std::vector<Poco::URI> target_hosts{toURIs(
        placement.placed(database, table),
        fmt::format(DDL_TABLE_PATCH_API_PATH_FMT, database, getURIEndpoint(record->headers), table),
        http_port)};

    if (target_hosts.empty())
    {
        LOG_ERROR(log, "Table {} is not found, payload={} query_id={} user={}", table, payload, query_id, user);
        failDDL(query_id, user, payload, "Table not found");
        return;
    }

    /// FIXME: make sure `target_hosts` is a complete list of hosts which
    /// has this table definition (shards * replication_factor)

    for (auto & uri : target_hosts)
    {
        uri.setQueryParameters(Poco::URI::QueryParameters{{"distributed_ddl", "false"}});
        doDDL(payload, uri, method, query_id);
    }

    succeedDDL(query_id, user, payload);
}

void DDLService::mutateDatabase(IDistributedWriteAheadLog::RecordPtr record, const String & method) const
{
    Block & block = record->block;
    assert(block.has("query_id"));

    if (!validateSchema(block, {"payload", "database", "timestamp", "query_id", "user"}))
    {
        return;
    }

    String payload = block.getByName("payload").column->getDataAt(0).toString();
    String database = block.getByName("database").column->getDataAt(0).toString();
    String query_id = block.getByName("query_id").column->getDataAt(0).toString();
    String user = block.getByName("user").column->getDataAt(0).toString();

    const auto & nodes = placement.nodes();
    if (nodes.empty())
    {
        LOG_ERROR(
            log,
            "Failed to mutage database because there are not enough hosts to place, payload={} "
            "query_id={} user={}",
            payload,
            query_id,
            user);
        failDDL(query_id, user, payload, "There are not enough hosts to place the table shard replicas");
        return;
    }

    std::vector<String> hosts;
    hosts.reserve(nodes.size());
    for (const auto & node : nodes)
    {
        hosts.push_back(node->host + ":" + node->http_port);
    }

    std::vector<Poco::URI> target_hosts{toURIs(hosts, fmt::format(DDL_DATABSE_POST_API_PATH_FMT, database), http_port)};

    /// FIXME: make sure `target_hosts` is a complete list of hosts which
    /// has this table definition (shards * replication_factor)

    /// FIXME : Parallelize doDDL on the uris
    for (auto & uri : target_hosts)
    {
        uri.setQueryParameters(Poco::URI::QueryParameters{{"distributed_ddl", "false"}});
        doDDL(payload, uri, method, query_id);
    }

    succeedDDL(query_id, user, payload);
}

void DDLService::commit(Int64 last_sn)
{
    /// FIXME, retries
    try
    {
        auto err = dwal->commit(last_sn, dwal_consume_ctx);
        if (unlikely(err != 0))
        {
            /// It is ok as next commit will override this commit if it makes through.
            /// If it failed and then crashes, we will redo and we will find resource
            /// already exists or resource not exists errors which shall be handled in
            /// DDL processing functions. In this case, for idempotent DDL like create
            /// table or delete table, it shall be OK. For alter table, it may depend ?
            LOG_ERROR(log, "Failed to commit offset={} error={}", last_sn, err);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to commit offset={} exception={}",
            last_sn,
            getCurrentExceptionMessage(true, true));
    }
}

void DDLService::processRecords(const IDistributedWriteAheadLog::RecordPtrs & records)
{
    for (auto & record : records)
    {
        if (record->op_code == IDistributedWriteAheadLog::OpCode::CREATE_TABLE)
        {
            createTable(record);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::DELETE_TABLE)
        {
            mutateTable(record, Poco::Net::HTTPRequest::HTTP_DELETE);

            /// Delete DWAL
            String database = record->block.getByName("database").column->getDataAt(0).toString();
            String table = record->block.getByName("table").column->getDataAt(0).toString();
            std::any ctx{DistributedWriteAheadLogKafkaContext{database + "." + table}};
            doDeleteDWal(ctx);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::ALTER_TABLE)
        {
            mutateTable(record, Poco::Net::HTTPRequest::HTTP_PATCH);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::CREATE_DATABASE)
        {
            mutateDatabase(record, Poco::Net::HTTPRequest::HTTP_POST);
        }
        else if (record->op_code == IDistributedWriteAheadLog::OpCode::DELETE_DATABASE)
        {
            mutateDatabase(record, Poco::Net::HTTPRequest::HTTP_DELETE);

            /// FIXME : Clean up tables DWAL in the database
        }
        else
        {
            assert(0);
            LOG_ERROR(log, "Unknown operation={}", static_cast<Int32>(record->op_code));
        }
    }

    const_cast<DDLService *>(this)->commit(records.back()->sn);

    /// FIXME, update DDL task status after committing offset / local offset checkpoint ...
}

}
