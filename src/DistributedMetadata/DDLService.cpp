#include "DDLService.h"

#include "CatalogService.h"
#include "PlacementService.h"
#include "TaskStatusService.h"
#include "sendRequest.h"

#include <Core/Block.h>
#include <DistributedWriteAheadLog/KafkaWAL.h>
#include <DistributedWriteAheadLog/KafkaWALCommon.h>
#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNRETRIABLE_ERROR;
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{
    /// Globals
    const String DDL_KEY_PREFIX = "cluster_settings.system_ddls.";
    const String DDL_DEFAULT_TOPIC = "__system_ddls";

    const String DDL_TABLE_POST_API_PATH_FMT = "/dae/v1/ddl/{}";
    const String DDL_TABLE_PATCH_API_PATH_FMT = "/dae/v1/ddl/{}/{}";
    const String DDL_TABLE_DELETE_API_PATH_FMT = "/dae/v1/ddl/{}/{}";
    const String DDL_COLUMN_POST_API_PATH_FMT = "/dae/v1/ddl/{}/columns";
    const String DDL_COLUMN_PATCH_API_PATH_FMT = "/dae/v1/ddl/{}/columns/{}";
    const String DDL_COLUMN_DELETE_API_PATH_FMT = "/dae/v1/ddl/{}/columns/{}";
    const String DDL_DATABSE_POST_API_PATH_FMT = "/dae/v1/ddl/databases";
    const String DDL_DATABSE_DELETE_API_PATH_FMT = "/dae/v1/ddl/databases/{}";

    constexpr Int32 MAX_RETRIES = 3;

    /// FIXME, add other un-retriable error codes
    const std::vector<String> UNRETRIABLE_ERROR_CODES = {
        "57", /// Table already exists.
        "60", /// Table does not exist.
        "62", /// Syntax error.
        "81", /// Database does not exist.
        "82", /// Database already  exists.
    };

    bool isUnretriableError(const String & err_msg)
    {
        for (const auto & err_code : UNRETRIABLE_ERROR_CODES)
        {
            if (err_msg.find("code:" + err_code) != String::npos || err_msg.find("Code: " + err_code) != String::npos)
            {
                return true;
            }
        }

        return false;
    }

    int toErrorCode(int http_code, const String & error_message)
    {
        if (http_code == Poco::Net::HTTPResponse::HTTP_OK)
        {
            return ErrorCodes::OK;
        }

        if (http_code < 0)
        {
            return ErrorCodes::UNRETRIABLE_ERROR;
        }

        return isUnretriableError(error_message) ? ErrorCodes::UNRETRIABLE_ERROR : ErrorCodes::UNKNOWN_EXCEPTION;
    }

    String getTableCategory(const std::unordered_map<String, String> & headers)
    {
        if (headers.contains("table_type") && headers.at("table_type") == "rawstore")
        {
            return "rawstores";
        }
        return "tables";
    }

    String getTableApiPath(const std::unordered_map<String, String> & headers, const String & table, const String & method)
    {
        if (method == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return fmt::format(DDL_TABLE_POST_API_PATH_FMT, getTableCategory(headers));
        }
        else if (method == Poco::Net::HTTPRequest::HTTP_PATCH)
        {
            return fmt::format(DDL_TABLE_PATCH_API_PATH_FMT, getTableCategory(headers), escapeForFileName(table));
        }
        else if (method == Poco::Net::HTTPRequest::HTTP_DELETE)
        {
            return fmt::format(DDL_TABLE_DELETE_API_PATH_FMT, getTableCategory(headers), escapeForFileName(table));
        }
        else
        {
            assert(false);
            return "";
        }
    }

    String getColumnApiPath(const std::unordered_map<String, String> & headers, const String & table, const String & method)
    {
        if (method == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return fmt::format(DDL_COLUMN_POST_API_PATH_FMT, escapeForFileName(table));
        }
        else if (method == Poco::Net::HTTPRequest::HTTP_PATCH)
        {
            return fmt::format(DDL_COLUMN_PATCH_API_PATH_FMT, escapeForFileName(table), escapeForFileName(headers.at("column")));
        }
        else if (method == Poco::Net::HTTPRequest::HTTP_DELETE)
        {
            return fmt::format(DDL_COLUMN_DELETE_API_PATH_FMT, escapeForFileName(table), escapeForFileName(headers.at("column")));
        }
        else
        {
            assert(false);
            return "";
        }
    }

    std::vector<Poco::URI> toURIs(const std::vector<String> & hosts, const String & path)
    {
        std::vector<Poco::URI> uris;
        uris.reserve(hosts.size());

        for (auto host : hosts)
        {
            /// FIXME : HTTP for now
            uris.emplace_back("http://" + host + path);
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
    , catalog(CatalogService::instance(global_context_))
    , placement(PlacementService::instance(global_context_))
    , task(TaskStatusService::instance(global_context_))
{
}

Int32 DDLService::append(const DWAL::Record & ddl_record) const
{
    return dwal->append(ddl_record, dwal_append_ctx).err;
}

MetadataService::ConfigSettings DDLService::configSettings() const
{
    return {
        .key_prefix = DDL_KEY_PREFIX,
        .default_name = DDL_DEFAULT_TOPIC,
        .default_data_retention = 168,
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

Int32 DDLService::doDDL(
    const String & payload, const Poco::URI & uri, const String & method, const String & query_id, const String & user) const
{
    const String & password = global_context->getPasswordByUserName(user);
    Int32 err = ErrorCodes::OK;

    for (auto i = 0; i < MAX_RETRIES; ++i)
    {
        auto [response, http_code] = sendRequest(uri, method, query_id, user, password, payload, log);

        err = toErrorCode(http_code, response);
        if (err == ErrorCodes::OK)
        {
            return err;
        }

        if (err == ErrorCodes::UNRETRIABLE_ERROR)
        {
            LOG_ERROR(log, "Failed to request uri={} due to unrecoverable failure, error_code={}", uri.toString(), err);
            return err;
        }

        if (i < MAX_RETRIES - 1)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000 * (2 << i)));
        }
    }

    LOG_ERROR(log, "Failed to send request to uri={} error_code={}", uri.toString(), err);
    return err;
}

void DDLService::doDDLOnHosts(std::vector<Poco::URI> & target_hosts, const String & payload,
                              const String & method, const String & query_id, const String & user) const
{
    std::vector<String> failed_hosts;
    /// FIXME : Parallelize doDDL on the uris
    for (auto & uri : target_hosts)
    {
        uri.addQueryParameter("distributed_ddl", "false");
        auto err = doDDL(payload, uri, method, query_id, user);
        if (err != ErrorCodes::OK)
        {
            failed_hosts.push_back(uri.getHost() + ":" + toString(uri.getPort()));
        }
    }

    if (failed_hosts.empty())
    {
        succeedDDL(query_id, user, payload);
    }
    else
    {
        failDDL(query_id, user, payload, "Failed to do DDL on hosts: " + boost::algorithm::join(failed_hosts, ","));
    }
}

void DDLService::createTable(DWAL::RecordPtr record)
{
    const Block & block = record->block;
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

    /// FIXME : check with catalog to see if this DDL is fulfilled
    /// Build a data structure to cached last 10000 DDLs, check against this data structure

    if (record->headers.contains("hosts"))
    {
        /// If `hosts` exists in the block, we already placed the replicas
        /// then we move to the execution stage

        const String * url_parameters = nullptr;
        if (record->headers.contains("url_parameters"))
        {
            url_parameters = &record->headers.at("url_parameters");
        }
        /// Create a DWAL for this table.
        try
        {
            createDWAL(database, table, shards, replication_factor, url_parameters);
        }
        catch (Exception e)
        {
            LOG_ERROR(log, "Failed to create topic for table payload={} exception={}", payload, e.message());
            failDDL(query_id, user, payload, e.message());
            return;
        }

        const String & hosts_val = record->headers.at("hosts");
        std::vector<String> hosts;
        boost::algorithm::split(hosts, hosts_val, boost::is_any_of(","));
        assert(!hosts.empty());

        std::vector<Poco::URI> target_hosts{toURIs(hosts, getTableApiPath(record->headers, table, Poco::Net::HTTPRequest::HTTP_POST))};

        /// Set the parameters in uris
        for (Int32 i = 0; i < replication_factor; ++i)
        {
            for (Int32 j = 0; j < shards; ++j)
            {
                auto & uri = target_hosts[i * shards + j];
                if (url_parameters != nullptr)
                {
                    uri.setRawQuery(*url_parameters);
                }
                uri.addQueryParameter("shard", std::to_string(j));
            }
        }
        /// Create table on each target host according to placement
        doDDLOnHosts(target_hosts, payload, Poco::Net::HTTPRequest::HTTP_POST, query_id, user);
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
            /// FIXME, https
            target_hosts.push_back(node->node.host + ":" + std::to_string(node->node.http_port));
        }

        /// We got the placement, commit the placement decision
        /// Add `hosts` into to record header
        String hosts{boost::algorithm::join(target_hosts, ",")};
        record->headers["hosts"] = hosts;

        auto result = append(*record.get());
        if (result != ErrorCodes::OK)
        {
            LOG_ERROR(log, "Failed to commit placement decision for create table payload={}", payload);
            failDDL(query_id, user, payload, "Internal server error");
            return;
        }

        LOG_INFO(log, "Successfully find placement for create table payload={} placement={}", payload, hosts);
        progressDDL(query_id, user, payload, "shard replicas placed");
    }
}

void DDLService::mutateTable(DWAL::RecordPtr record, const String & method) const
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

    auto target_hosts = getTargetURIs(record, database, table, method);

    if (target_hosts.empty())
    {
        LOG_ERROR(log, "Table {} is not found, payload={} query_id={} user={}", table, payload, query_id, user);
        failDDL(query_id, user, payload, "Table not found");
        return;
    }

    auto [replication_factor, shards] = catalog.shardAndReplicationFactor(database, table);
    auto total_replicas = replication_factor * shards;
    int hosts_size = target_hosts.size();

    if (hosts_size != total_replicas)
    {
        LOG_ERROR(
            log,
            "The number of table {} definitions is inconsistent with the actual obtained, payload={} query_id={} user={} "
            "total_replicas={} hosts_size={}",
            table,
            payload,
            query_id,
            user,
            total_replicas,
            hosts_size);
        failDDL(query_id, user, payload, "Table number obtained error");
        return;
    }

    doDDLOnHosts(target_hosts, payload, method, query_id, user);
}

void DDLService::mutateDatabase(DWAL::RecordPtr record, const String & method) const
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
        /// FIXME, https
        hosts.push_back(node->node.host + ":" + std::to_string(node->node.http_port));
    }

    const String * api_path_fmt = nullptr;
    if (method == Poco::Net::HTTPRequest::HTTP_POST)
    {
        api_path_fmt = &DDL_DATABSE_POST_API_PATH_FMT;
    }
    else if (method == Poco::Net::HTTPRequest::HTTP_DELETE)
    {
        api_path_fmt = &DDL_DATABSE_DELETE_API_PATH_FMT;
    }
    else
    {
        assert(false);
        LOG_ERROR(log, "Unsupported method={}", method);
        failDDL(query_id, user, payload, "Unsupported method " + method);
        return;
    }

    std::vector<Poco::URI> target_hosts{toURIs(hosts, fmt::format(*api_path_fmt, database))};

    doDDLOnHosts(target_hosts, payload, method, query_id, user);
}

void DDLService::commit(Int64 last_sn)
{
    for (auto i = 0; i < MAX_RETRIES; ++i)
    {
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
                LOG_ERROR(log, "Failed to commit offset={} error={} tried_times={}", last_sn, err, i);
            }
            else
            {
                return;
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to commit offset={} exception={}, tried_times={}", last_sn, getCurrentExceptionMessage(true, true), i);
        }

        if (i < MAX_RETRIES - 1)
        {
            LOG_INFO(log, "Sleep for a while and will try to commit offset={} again.", last_sn);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000 * (2 << i)));
        }
    }
}

void DDLService::processRecords(const DWAL::RecordPtrs & records)
{
    for (auto & record : records)
    {
        switch (record->op_code)
        {
            case DWAL::OpCode::CREATE_TABLE:
            {
                createTable(record);
                break;
            }
            case DWAL::OpCode::ALTER_TABLE:
            {
                mutateTable(record, Poco::Net::HTTPRequest::HTTP_PATCH);
                break;
            }
            case DWAL::OpCode::DELETE_TABLE:
            {
                mutateTable(record, Poco::Net::HTTPRequest::HTTP_DELETE);

                /// Delete DWAL
                String database = record->block.getByName("database").column->getDataAt(0).toString();
                String table = record->block.getByName("table").column->getDataAt(0).toString();
                DWAL::KafkaWALContext ctx{DWAL::escapeDWALName(database, table)};
                doDeleteDWal(ctx);
                break;
            }
            case DWAL::OpCode::CREATE_COLUMN:
            {
                mutateTable(record, Poco::Net::HTTPRequest::HTTP_POST);
                break;
            }
            case DWAL::OpCode::ALTER_COLUMN:
            {
                mutateTable(record, Poco::Net::HTTPRequest::HTTP_PATCH);
                break;
            }
            case DWAL::OpCode::DELETE_COLUMN:
            {
                mutateTable(record, Poco::Net::HTTPRequest::HTTP_DELETE);
                break;
            }
            case DWAL::OpCode::CREATE_DATABASE:
            {
                mutateDatabase(record, Poco::Net::HTTPRequest::HTTP_POST);
                break;
            }
            case DWAL::OpCode::DELETE_DATABASE:
            {
                mutateDatabase(record, Poco::Net::HTTPRequest::HTTP_DELETE);
                break;
            }
            default:
            {
                assert(0);
                LOG_ERROR(log, "Unknown operation={}", static_cast<Int32>(record->op_code));
            }
        }
    }

    const_cast<DDLService *>(this)->commit(records.back()->sn);

    /// FIXME, update DDL task status after committing offset / local offset checkpoint ...
}

std::vector<Poco::URI>
DDLService::getTargetURIs(DWAL::RecordPtr record, const String & database, const String & table, const String & method) const
{
    if (record->op_code == DWAL::OpCode::CREATE_COLUMN || record->op_code == DWAL::OpCode::ALTER_COLUMN
        || record->op_code == DWAL::OpCode::DELETE_COLUMN)
    {
        /// Column DDL request
        return toURIs(placement.placed(database, table), getColumnApiPath(record->headers, table, method));
    }
    else
    {
        /// Table DDL request
        return toURIs(placement.placed(database, table), getTableApiPath(record->headers, table, method));
    }
}

void DDLService::createDWAL(
    const String & database, const String & table, Int32 shards, Int32 replication_factor, const String * url_parameters) const
{
    DWAL::KafkaWALContext ctx{DWAL::escapeDWALName(database, table), shards, replication_factor, "delete"};

    /// Parse these settings from url parameters
    /// streaming_storage_retention_bytes,
    /// streaming_storage_retention_ms,
    /// streaming_storage_flush_messages,
    /// streaming_storage_flush_ms
    if (url_parameters != nullptr)
    {
        Poco::URI uri;
        uri.setRawQuery(*url_parameters);
        auto params = uri.getQueryParameters();

        for (const auto & kv : params)
        {
            if (kv.first == "streaming_storage_retention_bytes")
            {
                ctx.retention_bytes = std::stoll(kv.second);
            }
            else if (kv.first == "streaming_storage_retention_ms")
            {
                ctx.retention_ms = std::stoll(kv.second);
            }
            else if (kv.first == "streaming_storage_flush_messages")
            {
                ctx.flush_messages = std::stoll(kv.second);
            }
            else if (kv.first == "streaming_storage_flush_ms")
            {
                ctx.flush_ms = std::stoll(kv.second);
            }
        }
    }

    doCreateDWal(ctx);
}

}
