#pragma once

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Dictionaries/IDictionarySource.h>

#include <ClickHouse/ITypeNameProvider.h>
#include <ClickHouse/ConnectionPool.h>
#include <Interpreters/Context.h>

namespace DB
{
/** Allows loading dictionaries from local or remote ClickHouse instance
  *    @todo use ConnectionPoolWithFailover
  *    @todo invent a way to keep track of source modifications
  */
class ClickHouseDictionarySource final : public IDictionarySource,
                                         public std::enable_shared_from_this<ClickHouseDictionarySource>,
                                         public DB::ClickHouse::ITypeNameProvider
{
public:
    struct Configuration
    {
        const std::string host;
        const std::string user;
        const std::string password;
        const std::string quota_key;
        const std::string db;
        const std::string table;
        const std::string query;
        const std::string where;
        const std::string invalidate_query;
        const std::string update_field;
        const UInt64 update_lag;
        const UInt16 port;
        const bool is_local;
        const bool secure;
        const bool final;
    };

    ClickHouseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        const Block & sample_block_,
        ContextMutablePtr context_);

    /// copy-constructor is provided in order to support clone-ability
    ClickHouseDictionarySource(const ClickHouseDictionarySource & other);
    ClickHouseDictionarySource & operator=(const ClickHouseDictionarySource &) = delete;

    /// Returns the column name and type name pairs.
    const std::unordered_map<String, String> & getColumnTypeNames() const override { return original_column_type_names; }

    QueryPipeline loadAllWithSizeHint(std::atomic<size_t> * result_size_hint) override;

    QueryPipeline loadAll() override;

    QueryPipeline loadUpdatedAll() override;

    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;

    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;
    bool supportsSelectiveLoad() const override { return true; }

    bool hasUpdateField() const override;

    /// proton: starts
    /// CLICKHOUSE SOURCE is always remote.
    /// bool isLocal() const { return configuration.is_local; }
    /// proton: ends

    DictionarySourcePtr clone() const override { return std::make_shared<ClickHouseDictionarySource>(*this); }

    std::string toString() const override;

    /// Used for detection whether the hashtable should be preallocated
    /// (since if there is WHERE then it can filter out too much)
    bool hasWhere() const { return !configuration.where.empty(); }

private:
    std::string getUpdateFieldAndDate();

    QueryPipeline createStreamForQuery(const String & query, std::atomic<size_t> * result_size_hint = nullptr);

    std::string doInvalidateQuery(const std::string & request) const;

    /// To make ClickHouse::Client happy, not used since it is only for write
    std::unordered_map<String, String> original_column_type_names;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    mutable std::string invalidate_query_response;
    ExternalQueryBuilder query_builder;
    Block sample_block;
    ContextMutablePtr context;
    ClickHouse::ConnectionPoolPtr pool;
    const std::string load_all_query;
    LoggerPtr logger;
};

}
