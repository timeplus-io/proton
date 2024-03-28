#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/formatBlock.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/Streaming/HashJoin.h>
#include <Interpreters/Streaming/SyntaxAnalyzeUtils.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Transforms/Streaming/JoinTransform.h>
#include <base/constexpr_helpers.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <ranges>

#include <gtest/gtest.h>

/// BufferedStreamData:
/// using MapsOne = MapsTemplate<RowRefWithRefCount<Block>>;
/// using MapsMultiple = MapsTemplate<RowRefListMultiplePtr>;
/// using MapsAll = MapsTemplate<RowRefList>;
/// using MapsAsof = MapsTemplate<AsofRowRefs>;
/// using MapsRangeAsof = MapsTemplate<RangeAsofRowRefs>;
///
/// HashJoin::PrimaryKeyHashTable:
/// - MapsTemplate<RowRefListMultipleRefPtr>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int INVALID_JOIN_ON_EXPRESSION;
extern const int EXPECTED_ALL_OR_ANY;
extern const int UNSUPPORTED;
}

namespace
{
using namespace DB;

/// A copy from `TreeRewriter.cpp`
void setJoinStrictness(ASTSelectQuery & select_query, JoinStrictness join_default_strictness, bool old_any, ASTTableJoin & out_table_join)
{
    const ASTTablesInSelectQueryElement * node = select_query.join();
    if (!node)
        return;

    auto & table_join = const_cast<ASTTablesInSelectQueryElement *>(node)->table_join->as<ASTTableJoin &>();

    if (table_join.strictness == JoinStrictness::Unspecified && table_join.kind != JoinKind::Cross)
    {
        if (join_default_strictness == JoinStrictness::Any)
            table_join.strictness = JoinStrictness::Any;
        else if (join_default_strictness == JoinStrictness::All)
            table_join.strictness = JoinStrictness::All;
        else
            throw Exception(
                "Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty",
                DB::ErrorCodes::EXPECTED_ALL_OR_ANY);
    }

    if (old_any)
    {
        if (table_join.strictness == JoinStrictness::Any && table_join.kind == JoinKind::Inner)
        {
            table_join.strictness = JoinStrictness::Semi;
            table_join.kind = JoinKind::Left;
        }

        if (table_join.strictness == JoinStrictness::Any)
            table_join.strictness = JoinStrictness::RightAny;
    }
    else
    {
        if (table_join.strictness == JoinStrictness::Any && table_join.kind == JoinKind::Full)
            throw Exception("ANY FULL JOINs are not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    out_table_join = table_join;
}

/// A copy from `TreeRewriter.cpp`
/// Find the columns that are obtained by JOIN.
void collectJoinedColumns(
    TableJoin & analyzed_join, ASTTableJoin & table_join, const TablesWithColumns & tables, const Aliases & aliases, ContextPtr context)
{
    assert(tables.size() >= 2);

    if (table_join.using_expression_list)
    {
        const auto & keys = table_join.using_expression_list->as<ASTExpressionList &>();

        analyzed_join.addDisjunct();
        for (const auto & key : keys.children)
            analyzed_join.addUsingKey(key);
    }
    else if (table_join.on_expression)
    {
        bool is_asof = (table_join.strictness == JoinStrictness::Asof);

        CollectJoinOnKeysVisitor::Data data{analyzed_join, tables[0], tables[1], aliases, is_asof};
        if (auto * or_func = table_join.on_expression->as<ASTFunction>(); or_func && or_func->name == "or")
        {
            for (auto & disjunct : or_func->arguments->children)
            {
                analyzed_join.addDisjunct();
                CollectJoinOnKeysVisitor(data).visit(disjunct);
            }
            assert(analyzed_join.getClauses().size() == or_func->arguments->children.size());
        }
        else
        {
            analyzed_join.addDisjunct();
            CollectJoinOnKeysVisitor(data).visit(table_join.on_expression);
            assert(analyzed_join.oneDisjunct());
        }

        /// proton : starts. `date_diff_within` etc explicitly turns join to Asof join. During on key visitor, `is_asof` can be changed
        if (analyzed_join.getAsofInequality() == ASOFJoinInequality::RangeBetween)
        {
            if (!data.range_analyze_finished)
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "Range join requires specifying a range on join clause, but only lower bound or upper bound of a range is specified");

            analyzed_join.validateRangeAsof(context->getSettingsRef().max_join_range);
            analyzed_join.setStrictness(table_join.strictness);
        }
        /// proton : ends

        auto check_keys_empty = [](auto e) { return e.key_names_left.empty(); };

        /// All clauses should to have keys or be empty simultaneously
        bool all_keys_empty = std::all_of(analyzed_join.getClauses().begin(), analyzed_join.getClauses().end(), check_keys_empty);
        if (all_keys_empty)
        {
            /// Try join on constant (cross or empty join) or fail
            if (is_asof || analyzed_join.getAsofInequality() == ASOFJoinInequality::RangeBetween) /// proton : starts / ends
                throw Exception(
                    ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                    "ASOF or Range join requires at least one 'equal' join key in JOIN ON section: {}",
                    queryToString(table_join.on_expression));

            // bool join_on_const_ok = tryJoinOnConst(analyzed_join, table_join.on_expression, context);
            // if (!join_on_const_ok)
            //     throw Exception(
            //         ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            //         "Cannot get JOIN keys from JOIN ON section: {}",
            //         queryToString(table_join.on_expression));
        }
        else
        {
            bool any_keys_empty = std::any_of(analyzed_join.getClauses().begin(), analyzed_join.getClauses().end(), check_keys_empty);

            if (any_keys_empty)
                throw Exception(
                    ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                    "Cannot get JOIN keys from JOIN ON section: '{}'",
                    queryToString(table_join.on_expression));

            if (is_asof || analyzed_join.getAsofInequality() == ASOFJoinInequality::RangeBetween) /// proton : starts / ends
            {
                if (!analyzed_join.oneDisjunct())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join doesn't support multiple ORs for keys in JOIN ON section");
                data.asofToJoinKeys();
            }

            if (!analyzed_join.oneDisjunct() && !analyzed_join.isEnabledAlgorithm(JoinAlgorithm::HASH))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `hash` join supports multiple ORs for keys in JOIN ON section");
        }
    }
}

std::shared_ptr<TableJoin> initTableJoin(
    std::string_view kind,
    std::string_view strictness,
    std::string_view on_clause,
    const Block & left_header,
    Streaming::DataStreamSemanticEx left_data_stream_semantic,
    const Block & right_header,
    Streaming::DataStreamSemanticEx right_data_stream_semantic,
    ContextPtr context)
{
    ParserSelectQuery parser;
    auto select = parseQuery(
        parser,
        fmt::format("select t1.*, t2.* from t1 {} {} JOIN t2 ON {}", kind, strictness, on_clause),
        0,
        DBMS_DEFAULT_MAX_PARSER_DEPTH);
    auto * join = select->as<ASTSelectQuery &>().join();
    auto & ast_table_join = join->table_join->as<ASTTableJoin &>();

    DatabaseAndTableWithAlias ltable;
    ltable.table = "t1";
    TableWithColumnNamesAndTypes left_table{std::move(ltable), left_header.getNamesAndTypesList()};
    left_table.setOutputDataStreamSemantic(left_data_stream_semantic);

    DatabaseAndTableWithAlias rtable;
    rtable.table = "t2";
    TableWithColumnNamesAndTypes right_table{std::move(rtable), right_header.getNamesAndTypesList()};
    right_table.setOutputDataStreamSemantic(right_data_stream_semantic);

    NameSet source_columns_set;
    std::ranges::for_each(left_header, [&](const auto & column_with_type_name) { source_columns_set.insert(column_with_type_name.name); });
    std::ranges::for_each(right_header, [&](const auto & column_with_type_name) { source_columns_set.insert(column_with_type_name.name); });

    TablesWithColumns tables_with_columns{std::move(left_table), std::move(right_table)};

    TranslateQualifiedNamesVisitor::Data visitor_data(std::move(source_columns_set), tables_with_columns);
    TranslateQualifiedNamesVisitor(visitor_data).visit(select);

    ASTTableJoin out_table_join;
    setJoinStrictness(
        select->as<ASTSelectQuery &>(),
        context->getSettingsRef().join_default_strictness,
        context->getSettingsRef().any_join_distinct_right_table_keys,
        out_table_join);

    auto table_join = std::make_shared<TableJoin>();
    table_join->setTableJoin(std::move(out_table_join));

    collectJoinedColumns(*table_join, ast_table_join, tables_with_columns, {}, context);

    table_join->setTablesWithColumns(std::move(tables_with_columns));

    return table_join;
}

Streaming::HashJoinPtr initHashJoin(
    std::shared_ptr<TableJoin> table_join, const Block & left_header, const Block & right_header, UInt64 keep_versions, ContextPtr context)
{
    const auto & tables = table_join->getTablesWithColumns();
    assert(tables.size() == 2);

    auto left_join_stream_desc = std::make_shared<Streaming::JoinStreamDescription>(
        tables[0], Block{}, tables[0].output_data_stream_semantic, keep_versions, 0, 0);
    auto right_join_stream_desc = std::make_shared<Streaming::JoinStreamDescription>(
        tables[1], right_header, tables[1].output_data_stream_semantic, keep_versions, 0, 0);
    right_join_stream_desc->calculateColumnPositions(table_join->strictness());

    auto join = Streaming::HashJoin::create(table_join, std::move(left_join_stream_desc), std::move(right_join_stream_desc));

    auto output_header
        = Streaming::JoinTransform::transformHeader(left_header.cloneEmpty(), std::dynamic_pointer_cast<Streaming::IHashJoin>(join));
    join->postInit(left_header, output_header, context->getSettingsRef().join_max_buffered_bytes);

    return join;
}

void serdeAndCheck(const Streaming::IHashJoin & join, Streaming::IHashJoin & recovered_join, std::string_view msg)
{
    WriteBufferFromOwnString wb;
    join.serialize(wb, ProtonRevision::getVersionRevision());
    auto original_string = wb.str();

    ReadBufferFromOwnString rb(original_string);
    recovered_join.deserialize(rb, ProtonRevision::getVersionRevision());

    WriteBufferFromOwnString wb2;
    recovered_join.serialize(wb2, ProtonRevision::getVersionRevision());
    auto recovered_string = wb2.str();

    ASSERT_EQ(original_string, recovered_string) << msg << ": (FAILED)\n";
}

Block prepareBlockByHeader(const Block & header, std::string_view values_buf, ContextPtr context)
{
    tryRegisterFormats();

    if (values_buf.empty())
        return header.cloneEmpty();

    ReadBufferFromString rb(values_buf);
    auto input_format = context->getInputFormat("Values", rb, header, DEFAULT_BLOCK_SIZE, /*FormatSettings*/ std::nullopt);
    if (auto * row_fmt = dynamic_cast<IRowInputFormat *>(input_format.get()))
        row_fmt->setMaxSize(DEFAULT_BLOCK_SIZE);

    return input_format->read(DEFAULT_BLOCK_SIZE, /*timeout_ms*/ 10000);
}

Block prepareBlock(const Strings & types, std::string_view values_buf, ContextPtr context)
{
    Block header;
    for (size_t i = 0; const auto & type : types)
        header.insert(ColumnWithTypeAndName(DataTypeFactory::instance().get(type), fmt::format("col_{}", ++i)));

    return prepareBlockByHeader(header, values_buf, context);
}

Block prepareBlockWithDelta(const Strings & types, std::string_view values_buf, ContextPtr context)
{
    assert(types.size() > 0);
    Block header;
    for (size_t i = 0; const auto & type : types)
    {
        ++i;
        if (i == types.size())
        {
            /// last column is _tp_delta
            assert(type == "int8");
            header.insert(ColumnWithTypeAndName(DataTypeFactory::instance().get(type), "_tp_delta"));
        }
        else
            header.insert(ColumnWithTypeAndName(DataTypeFactory::instance().get(type), fmt::format("col_{}", i)));
    }

    return prepareBlockByHeader(header, values_buf, context);
}

struct JoinResults
{
    Block block;
    Block retracted_block;
};

struct ExpectedJoinResults
{
    String values;
    String retracted_values;

    JoinResults toJoinResults(const Block & header, ContextPtr context)
    {
        JoinResults results;
        if (!values.empty())
            results.block = prepareBlockByHeader(header, values, context);

        if (!retracted_values.empty())
            results.retracted_block = prepareBlockByHeader(header, retracted_values, context);

        return results;
    }
};

void checkJoinResults(const JoinResults & actual_results, const JoinResults & expected_results, std::string_view msg, ContextPtr context)
{
    auto check_block = [&](const Block & actual, const Block & expect, std::string_view msg_prefix) {
        SipHash hash_expected;
        expect.updateHash(hash_expected);

        SipHash hash_got;
        actual.updateHash(hash_got);

        bool checked = hash_expected.get64() == hash_got.get64();
        if (!checked)
        {
            WriteBufferFromOwnString wb;
            {
                wb << "Expect block:\n";
                auto output_format = context->getOutputFormat("PrettyCompact", wb, expect);
                formatBlock(output_format, expect);
            }
            {
                wb << "\nActual block:\n";
                auto output_format = context->getOutputFormat("PrettyCompact", wb, actual);
                formatBlock(output_format, actual);
            }
            std::cerr << wb.str() << std::endl;
        }

        ASSERT_TRUE(checked) << msg_prefix << ": (FAILED)\n";
    };

    check_block(actual_results.block, expected_results.block, fmt::format("{}\n check the joined block of join results", msg));
    check_block(actual_results.retracted_block, expected_results.retracted_block, fmt::format("{}\n check the retracted block", msg));
}

struct ToJoinStep
{
    enum
    {
        LEFT,
        RIGHT
    } pos;

    Block block;

    std::optional<ExpectedJoinResults> expected_results;
};

void commonTest(
    std::string_view kind,
    std::string_view strictness,
    std::string_view on_clause,
    Block left_header,
    Streaming::DataStreamSemanticEx left_data_stream_semantic,
    std::optional<std::vector<size_t>> left_primary_key_column_indexes,
    Block right_header,
    Streaming::DataStreamSemanticEx right_data_stream_semantic,
    std::optional<std::vector<size_t>> right_primary_key_column_indexes,
    std::vector<ToJoinStep> to_join_steps,
    ContextPtr context)
{
    auto lower_strictness = Poco::toLower(String(strictness));
    if (lower_strictness != "asof" && lower_strictness != "any" && lower_strictness != "latest")
    {
        if (Streaming::isVersionedKeyedStorage(left_data_stream_semantic))
            left_data_stream_semantic = Streaming::DataStreamSemantic::Changelog;

        if (Streaming::isVersionedKeyedStorage(right_data_stream_semantic))
            right_data_stream_semantic = Streaming::DataStreamSemantic::Changelog;
    }

    auto msg = fmt::format(
        "(Test case: <{}> {} {} JOIN <{}>, ON clause: {}, left header: '{}', right header: '{}')",
        magic_enum::enum_name(left_data_stream_semantic.toStorageSemantic()),
        kind,
        strictness,
        magic_enum::enum_name(right_data_stream_semantic.toStorageSemantic()),
        on_clause,
        left_header.dumpStructure(),
        right_header.dumpStructure());

    try
    {
        auto convert_block
            = [&](Streaming::DataStreamSemanticEx data_stream_semantic, const auto & primary_key_column_indexes, Block & block) -> Block & {
            /// So far not support primary key column + version column
            // if (Streaming::isKeyedStorage(data_stream_semantic))
            // {
            //     assert(primary_key_column_indexes.has_value());
            //     /// Add primary key column
            //     for (auto index : primary_key_column_indexes.value())
            //     {
            //         const auto & column = block.getByPosition(index);
            //         if (!column.name.starts_with(ProtonConsts::PRIMARY_KEY_COLUMN_PREFIX))
            //         {
            //             auto key_column = column;
            //             key_column.name = ProtonConsts::PRIMARY_KEY_COLUMN_PREFIX + key_column.name;
            //             block.insert(std::move(key_column));
            //         }
            //     }

            //     /// Add version column
            //     if (!std::ranges::any_of(
            //             block, [](const auto & column) { return column.name.starts_with(ProtonConsts::VERSION_COLUMN_PREFIX); }))
            //     {
            //         auto columns = block.getColumnsWithTypeAndName();
            //         auto iter = columns.rbegin();
            //         for (; iter != columns.rend(); ++iter)
            //         {
            //             if (isDateTime(iter->type) || isDateTime64(iter->type))
            //             {
            //                 auto version_column = *iter;
            //                 version_column.name = ProtonConsts::VERSION_COLUMN_PREFIX + version_column.name;
            //                 block.insert(std::move(version_column));
            //                 break;
            //             }
            //         }
            //         EXPECT_TRUE(iter != columns.rend()) << msg << ": missing datetime column as version column (FAILED)\n";
            //     }
            // }
            if (!Streaming::isAppendDataStream(data_stream_semantic))
            {
                /// Add _tp_delta if not exists
                if (!std::ranges::any_of(block, [](const auto & column) { return column.name == ProtonConsts::RESERVED_DELTA_FLAG; }))
                    block.insert(ColumnWithTypeAndName{
                        ColumnInt8::create(block.rows(), 1), std::make_shared<DataTypeInt8>(), ProtonConsts::RESERVED_DELTA_FLAG});
            }
            return block;
        };

        left_header = convert_block(left_data_stream_semantic, left_primary_key_column_indexes, left_header);
        right_header = convert_block(right_data_stream_semantic, right_primary_key_column_indexes, right_header);

        auto table_join = initTableJoin(
            kind, strictness, on_clause, left_header, left_data_stream_semantic, right_header, right_data_stream_semantic, context);

        UInt64 keep_versions = context->getSettingsRef().keep_versions;

        auto convert_left_block
            = [&](Block & block) -> Block & { return convert_block(left_data_stream_semantic, left_primary_key_column_indexes, block); };
        auto convert_right_block = [&](Block & block) -> Block & {
            convert_block(right_data_stream_semantic, right_primary_key_column_indexes, block);
            /// Add table prefix for right table
            for (auto & column : block)
                column.name = "t2." + column.name;
            block = Block(block.getColumnsWithTypeAndName());
            return block;
        };

        auto join = initHashJoin(table_join, convert_left_block(left_header), convert_right_block(right_header), keep_versions, context);
        auto output_header = join->getOutputHeader();

        auto do_join_step = [&](Streaming::IHashJoin & join_, ToJoinStep to_join_step) -> JoinResults {
            auto & [pos_, block_, _] = to_join_step;
            std::pair<LightChunk, LightChunk> join_result;
            if (pos_ == ToJoinStep::LEFT)
                join_result = join_.insertLeftDataBlockAndJoin(convert_left_block(block_));
            else
                join_result = join_.insertRightDataBlockAndJoin(convert_right_block(block_));

            auto & [retracted_block, block] = join_result;
            JoinResults results;
            if (join_result.second.rows() > 0)
                results.block = output_header.cloneWithColumns(block.detachColumns());

            if (retracted_block.rows() > 0)
                results.retracted_block = output_header.cloneWithColumns(retracted_block.detachColumns());

            return results;
        };

        /// Serde and check initiailized hash join
        auto join_for_recover = initHashJoin(table_join, left_header, right_header, keep_versions, context);
        serdeAndCheck(*join, *join_for_recover, fmt::format("{}\nCheck serialized/deserialized initiailized hash join", msg));

        /// Do join steps
        for (size_t i = 0; auto & to_join_step : to_join_steps)
        {
            auto join_results = do_join_step(*join, to_join_step);
            auto join_results2 = do_join_step(*join_for_recover, to_join_step);

            /// Check expect results
            if (to_join_step.expected_results.has_value())
            {
                JoinResults expected_join_results;
                ASSERT_NO_THROW(expected_join_results = to_join_step.expected_results->toJoinResults(join->getOutputHeader(), context))
                    << "Expected output header: " << join->getOutputHeader().dumpStructure() << "\n";
                checkJoinResults(
                    join_results, expected_join_results, fmt::format("{}\nDo join step-{}:\nCheck expect join results", msg, i), context);
            }

            /// Check serialized/deserialized results
            checkJoinResults(
                join_results2,
                join_results,
                fmt::format("{}\nDo join step-{}:\n Check serialized/deserialized join results", msg, i),
                context);

            /// Serde and check updated hash join
            join_for_recover = initHashJoin(table_join, left_header, right_header, keep_versions, context);
            serdeAndCheck(*join, *join_for_recover, fmt::format("{}\nDo join step-{}:\n Check serialized/deserialized hash join", msg, i));

            ++i;
        }
    }
    catch (DB::Exception & e)
    {
        e.addMessage("\nWhile running {}", msg);
        e.rethrow();
    }
}

template <typename JoinTest>
    requires(std::is_invocable_v<JoinTest, Streaming::DataStreamSemanticEx, JoinKind, JoinStrictness, Streaming::DataStreamSemanticEx>)
void dispatchJoinTests(JoinTest && join_test)
{
    /// A 4 dimensions array : Left DataStreamSemantic, JoinKind, JoinStrictness, Right DataStreamSemantic
    /// NOTE: Skip JoinStrictness::Unspecified and JoinStrictness::RightAny.
    constexpr auto total_combinations = magic_enum::enum_count<Streaming::StorageSemantic>() * magic_enum::enum_count<JoinKind>()
        * (magic_enum::enum_count<JoinStrictness>() - 2) * magic_enum::enum_count<Streaming::StorageSemantic>();
    constexpr auto dim3_combinations = total_combinations / magic_enum::enum_count<Streaming::StorageSemantic>();
    constexpr auto dim2_combinations = dim3_combinations / magic_enum::enum_count<JoinKind>();
    constexpr auto dim1_combinations = dim2_combinations / (magic_enum::enum_count<JoinStrictness>() - 2);

    ///  static_for<0, total_combinations>([&](auto index)
    for (size_t index = 0; index < total_combinations; ++index)
    {
        auto left_storage_semantic = magic_enum::enum_value<Streaming::StorageSemantic>(index / dim3_combinations);
        auto kind = magic_enum::enum_value<JoinKind>(index % dim3_combinations / dim2_combinations);
        auto strictness = magic_enum::enum_value<JoinStrictness>(index % dim3_combinations % dim2_combinations / dim1_combinations + 2);
        auto right_storage_semantic
            = magic_enum::enum_value<Streaming::StorageSemantic>(index % dim3_combinations % dim2_combinations % dim1_combinations);

        /// Retrieve streaming hash join support matrix
        bool supported = false;
        auto join_combination = std::make_tuple(left_storage_semantic, kind, strictness, right_storage_semantic);
        if (auto iter = Streaming::HashJoin::support_matrix.find(join_combination); iter != Streaming::HashJoin::support_matrix.end())
            supported = iter->second;

        try
        {
            join_test(left_storage_semantic, kind, strictness, right_storage_semantic);
            if (!supported)
            {
                FAIL() << fmt::format(
                    "Unsupported test case (but passed): (<{}> {} {} JOIN <{}>) in join test '{}'",
                    magic_enum::enum_name(left_storage_semantic),
                    kind,
                    strictness,
                    magic_enum::enum_name(right_storage_semantic),
                    typeid(join_test).name());
            }
            else
                SUCCEED();
        }
        catch (const DB::Exception & e)
        {
            if (!supported)
            {
                if (e.code() == ErrorCodes::NOT_IMPLEMENTED || e.code() == ErrorCodes::SYNTAX_ERROR || e.code() == ErrorCodes::UNSUPPORTED)
                {
                    SUCCEED();
                    continue;
                }
                /// Got an unexpected exception, it's failure
            }

            FAIL() << fmt::format(
                "Failure test case: (<{}> {} {} JOIN <{}>) in join test '{}', it throws an exception \"{}\"",
                magic_enum::enum_name(left_storage_semantic),
                kind,
                strictness,
                magic_enum::enum_name(right_storage_semantic),
                typeid(join_test).name(),
                getExceptionMessage(e, true));
        }
        catch (...)
        {
            FAIL() << fmt::format(
                "Failure test case: (<{}> {} {} JOIN <{}>) in join test '{}', it throws an exception \"{}\"",
                magic_enum::enum_name(left_storage_semantic),
                kind,
                strictness,
                magic_enum::enum_name(right_storage_semantic),
                typeid(join_test).name(),
                getCurrentExceptionMessage(true));
        }
    }
}
}

/// Common test table structure:
/// Left Table: t1
/// Columns:    col_1   col_2   ...
///
/// Right Table: t2
/// Columns:    col_1   col_2   ...
///
/// Result Columns:
/// t1.col_1, t1.col_2, t1. ... + t2.col_1, t2.col_2, t2. ...

TEST(StreamingHashJoin, CommonTest)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// stream(t1) join stream(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*default join kind*/ "",
        /*default join strictness*/ "",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Append,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, SimpleJoinTests)
{
    Strings supported_types = {
        "bool",
        "int8",
        "int16",
        "int32",
        "int64",
        "int128",
        "int256",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "uint128",
        "uint256",
        "float32",
        "float64",
        "decimal32(3)",
        "decimal64(3)",
        "decimal128(3)",
        "decimal256(3)",
        "string",
        "fixed_string(8)",
        "enum8('a' = 1, 'b' = 2, 'c' = 3)",
        "enum16('a' = 1, 'b' = 2, 'c' = 3)",
        "uuid",
        "date",
        "date32",
        "datetime",
        "datetime64(3, 'UTC')",
        "int8" /*used for _tp_delta*/
    };

    String default_values[] = {
        "(true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.111, 1.111, 1.111, 1.111, 'test1', 'test1', 'a', 'a', "
        "'4c6a2a19-4f9f-456f-b076-c43ef97255d1', '2023-1-1', '2023-1-1', '2023-1-1 00:00:01', '2023-1-1 00:00:00.001', +1)",
        "(true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.111, 1.111, 1.111, 1.111, 'test1', 'test1', 'a', 'a', "
        "'4c6a2a19-4f9f-456f-b076-c43ef97255d1', '2023-1-1', '2023-1-1', '2023-1-1 00:00:01', '2023-1-1 00:00:00.001', -1)",
        "(false, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.222, 2.222, 2.222, 2.222, 'test2', 'test2', 'b', 'b', "
        "'4c6a2a19-4f9f-456f-b076-c43ef97255d2', '2023-1-2', '2023-1-2', '2023-1-1 00:00:02', '2023-1-1 00:00:00.002', +1)",
    };

    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ supported_types, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ supported_types, /*no data*/ "", context);

    std::vector<ToJoinStep> to_join_steps
        = {{
               /*to join pos*/ ToJoinStep::RIGHT,
               /*to join block*/ prepareBlockByHeader(right_header, default_values[0], context),
               /*expected join results*/ std::nullopt, /// No check
           },
           {
               /*to join pos*/ ToJoinStep::LEFT,
               /*to join block*/ prepareBlockByHeader(left_header, default_values[0], context),
               /*expected join results*/ std::nullopt, /// No check
           },
           {
               /*to join pos*/ ToJoinStep::RIGHT,
               /*to join block*/ prepareBlockByHeader(right_header, default_values[1], context),
               /*expected join results*/ std::nullopt, /// No check
           },
           {
               /*to join pos*/ ToJoinStep::LEFT,
               /*to join block*/ prepareBlockByHeader(left_header, default_values[1], context),
               /*expected join results*/ std::nullopt, /// No check
           },
           {
               /*to join pos*/ ToJoinStep::RIGHT,
               /*to join block*/ prepareBlockByHeader(right_header, default_values[2], context),
               /*expected join results*/ std::nullopt, /// No check
           },
           {
               /*to join pos*/ ToJoinStep::LEFT,
               /*to join block*/ prepareBlockByHeader(left_header, default_values[2], context),
               /*expected join results*/ std::nullopt, /// No check
           }};

    /// For each type as join key: `col_x = col_x`, skip last column `_tp_delta`
    for (size_t i = 1; i <= supported_types.size() - 1; ++i)
    {
        auto on_clauses = fmt::format("t1.col_{} = t2.col_{}", i, i);

        /// back column type is `datetime64(3, 'UTC')`
        ASSERT_TRUE(supported_types.at(supported_types.size() - 2).starts_with("datetime"));
        auto time_col_i = supported_types.size() - 1;
        auto on_clauses_for_asof_join = fmt::format("t1.col_{} = t2.col_{} and t1.col_{} > t2.col_{}", i, i, time_col_i, time_col_i);

        auto on_clauses_for_range_join
            = fmt::format("t1.col_{} = t2.col_{} and date_diff_within(2s, t1.col_{}, t2.col_{})", i, i, time_col_i, time_col_i);

        dispatchJoinTests([&](auto left_data_stream_semantic, auto kind, auto strictness, auto right_data_stream_semantic) {
            std::optional<std::vector<size_t>> left_primary_key_column_indexes;
            std::optional<std::vector<size_t>> right_primary_key_column_indexes;
            if (Streaming::isKeyedStorage(left_data_stream_semantic))
                left_primary_key_column_indexes = std::vector<size_t>{i - 1};

            if (Streaming::isKeyedStorage(right_data_stream_semantic))
                right_primary_key_column_indexes = std::vector<size_t>{i - 1};

            commonTest(
                toString(kind),
                toString(strictness),
                strictness == JoinStrictness::Asof ? on_clauses_for_asof_join : on_clauses,
                left_header,
                left_data_stream_semantic,
                std::move(left_primary_key_column_indexes),
                right_header,
                right_data_stream_semantic,
                std::move(right_primary_key_column_indexes),
                to_join_steps,
                context);

            /// Additional range between
            if (Streaming::isAppendStorage(left_data_stream_semantic) && kind == JoinKind::Inner && strictness == JoinStrictness::All
                && Streaming::isAppendStorage(right_data_stream_semantic))
            {
                commonTest(
                    toString(kind),
                    toString(strictness),
                    on_clauses_for_range_join,
                    left_header,
                    left_data_stream_semantic,
                    std::nullopt,
                    right_header,
                    right_data_stream_semantic,
                    std::nullopt,
                    to_join_steps,
                    context);
            }
        });
    }
}

TEST(StreamingHashJoin, AppendLeftAsofJoinAppend)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    commonTest(
        "left",
        "asof",
        /*on_clause*/ "t1.col_1 = t2.col_1 and t1.col_2 > t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Append,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '1970-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:01', '2023-1-1 00:00:00')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftLatestJoinAppend)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// stream(t1) left latest join stream(t2) on t1.col_1 = t2.col_1
    commonTest(
        "left",
        "latest",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Append,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(2, '2023-1-1 00:00:00', '1970-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:01', '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftAllJoinChangelog)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Single primary key
    /// Append JOIN Changelog
    /// stream(t1) left all join changelog(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Changelog,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(2, '2023-1-1 00:00:00', '1970-1-1 00:00:00', 0)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftAllJoinChangelogKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Single primary key
    /// Append JOIN ChangelogKV
    /// stream(t1) left all join changelog_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(2, '2023-1-1 00:00:00', '1970-1-1 00:00:00', 0)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftAllJoinVersionedKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Single primary key
    /// Append JOIN VersionedKV
    /// stream(t1) left all join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(2, '2023-1-1 00:00:00', '1970-1-1 00:00:00', 0)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(1, '2023-1-1 00:00:01', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftAllJoinVersionedKVWithFullMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on full primary keys
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) left all join versioned_kv(t2) on t1.col_1 = t2.col_1 and t1.col_2 = t2.col_2
    commonTest(
        /*default join kind*/ "left",
        /*default join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1 and t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't2', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3, t2._tp_delta
                    .values = "(1, 't2', '2023-1-1 00:00:00', '1970-1-1 00:00:00', 0)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3, t2._tp_delta
                    .values = "(1, 't1', '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftAllJoinVersionedKVWithPartialMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on partial primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) left all join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3, t2._tp_delta
                    .values = "(2, 't1', '2023-1-1 00:00:00', '', '1970-1-1 00:00:00', 0)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(1, 'tt2', '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't2', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, t2._tp_delta
                    .values = "(1, 't2', '2023-1-1 00:00:01', 'tt1', '2023-1-1 00:00:01', 1)"
                              "(1, 't2', '2023-1-1 00:00:01', 'tt2', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftAllJoinVersionedKVWithNoPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on no primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) left all join versioned_kv(t2) on t1.col_2 = t2.col_2
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't2', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, t2._tp_delta
                    .values = "(1, 't2', '2023-1-1 00:00:00', 0, '1970-1-1 00:00:00', 0)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 'tt1', '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/
                prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, t2._tp_delta
                    .values = "(1, 'tt1', '2023-1-1 00:00:01', 1, '2023-1-1 00:00:01', 1)"
                              "(1, 'tt1', '2023-1-1 00:00:01', 2, '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftLatestJoinVersionedKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// Single primary key
    /// Append JOIN VersionedKV
    /// stream(t1) left latest join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "latest",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(2, '2023-1-1 00:00:00', '1970-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftLatestJoinVersionedKVWithPartialMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// Join on partial primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) left latest join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "latest",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3
                    .values = "(2, 't1', '2023-1-1 00:00:00', '', '1970-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01')(1, 'tt2', '2023-1-1 00:00:01')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't2', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3
                    .values = "(1, 't2', '2023-1-1 00:00:01', 'tt2', '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendLeftLatestJoinVersionedKVWithNoPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// Join on no primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) left latest join versioned_kv(t2) on t1.col_2 = t2.col_2
    commonTest(
        /*join kind*/ "left",
        /*join strictness*/ "latest",
        /*on_clause*/ "t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(2, 't2', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3
                    .values = "(2, 't2', '2023-1-1 00:00:00', 0, '1970-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01')(2, 'tt1', '2023-1-1 00:00:01')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/
                prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3
                    .values = "(1, 'tt1', '2023-1-1 00:00:01', 2, '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerAllJoinAppend)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// stream(t1) inner all join stream(t2) on t1.col_1 = t2.col_1
    commonTest(
        "inner",
        "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Append,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerRangeJoinAppend)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    commonTest(
        "inner",
        "all",
        /*on_clause*/ "t1.col_1 = t2.col_1 and date_diff_within(2s, t1.col_2, t2.col_2)",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Append,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:02')(1, '2023-1-1 00:00:03')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:02')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')(1, '2023-1-1 00:00:02')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')"
                              "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01')"
                              "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:02')"
                              "(1, '2023-1-1 00:00:02', '2023-1-1 00:00:00')"
                              "(1, '2023-1-1 00:00:02', '2023-1-1 00:00:01')"
                              "(1, '2023-1-1 00:00:02', '2023-1-1 00:00:02')"
                              "(1, '2023-1-1 00:00:02', '2023-1-1 00:00:03')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerAsofJoinAppend)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    commonTest(
        "inner",
        "asof",
        /*on_clause*/ "t1.col_1 = t2.col_1 and t1.col_2 >= t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Append,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerLatestJoinAppend)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// stream(t1) inner latest join stream(t2) on t1.col_1 = t2.col_1
    commonTest(
        "inner",
        "latest",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Append,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:01', '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerAllJoinChangelogKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Single primary key
    /// Append JOIN ChangelogKV
    /// stream(t1) inner all join changelog_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', 1)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerAllJoinVersionedKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Single primary key
    /// Append JOIN VersionedKV
    /// stream(t1) inner all join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', 1)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2, t2._tp_delta
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerAllJoinVersionedKVWithFullMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on full primary keys
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) inner all join versioned_kv(t2) on t1.col_1 = t2.col_1 and t1.col_2 = t2.col_2
    commonTest(
        /*default join kind*/ "inner",
        /*default join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1 and t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3, t2._tp_delta
                    .values = "(1, 't1', '2023-1-1 00:00:00', '2023-1-1 00:00:00', 1)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3, t2._tp_delta
                    .values = "(1, 't1', '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerAllJoinVersionedKVWithPartialMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on partial primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) inner all join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3, t2._tp_delta
                    .values = "(1, 't1', '2023-1-1 00:00:00', 'tt1', '2023-1-1 00:00:00', 1)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(1, 'tt2', '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't2', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, t2._tp_delta
                    .values = "(1, 't2', '2023-1-1 00:00:01', 'tt1', '2023-1-1 00:00:01', 1)"
                              "(1, 't2', '2023-1-1 00:00:01', 'tt2', '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerAllJoinVersionedKVWithNoPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on no primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) inner all join versioned_kv(t2) on t1.col_2 = t2.col_2
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, t2._tp_delta
                    .values = "(1, 't1', '2023-1-1 00:00:00', 1, '2023-1-1 00:00:00', 1)",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 'tt1', '2023-1-1 00:00:01', +1)", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/
                prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, t2._tp_delta
                    .values = "(1, 'tt1', '2023-1-1 00:00:01', 1, '2023-1-1 00:00:01', 1)"
                              "(1, 'tt1', '2023-1-1 00:00:01', 2, '2023-1-1 00:00:01', 1)",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerLatestJoinVersionedKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// Single primary key
    /// Append JOIN VersionedKV
    /// stream(t1) inner latest join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "latest",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, t2.col_2
                    .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerLatestJoinVersionedKVWithPartialMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// Join on partial primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) inner latest join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "latest",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_3
                    .values = "(1, 't1', '2023-1-1 00:00:00', 'tt1', '2023-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01')(1, 'tt2', '2023-1-1 00:00:01')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't2', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3
                    .values = "(1, 't2', '2023-1-1 00:00:01', 'tt2', '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, AppendInnerLatestJoinVersionedKVWithNoPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);
    Block right_header = prepareBlock(/*types*/ {"int", "string", "datetime64(3, 'UTC')"}, /*no data*/ "", context);

    /// Join on no primary key(s)
    /// Append JOIN VersionedKV (with multiple primary keys)
    /// stream(t1) inner latest join versioned_kv(t2) on t1.col_2 = t2.col_2
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "latest",
        /*on_clause*/ "t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::Append,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3
                    .values = "(1, 't1', '2023-1-1 00:00:00', 1, '2023-1-1 00:00:00')",
                },
            },
            {
                /*to join pos*/ ToJoinStep::RIGHT,
                /*to join block*/
                prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01')(2, 'tt1', '2023-1-1 00:00:01')", context),
                /*expected join results*/ ExpectedJoinResults{},
            },
            {
                /*to join pos*/ ToJoinStep::LEFT,
                /*to join block*/
                prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01')", context),
                /*expected join results*/
                ExpectedJoinResults{
                    /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3
                    .values = "(1, 'tt1', '2023-1-1 00:00:01', 2, '2023-1-1 00:00:01')",
                },
            },
        },
        context);
}

TEST(StreamingHashJoin, VersionedKVInnerAllJoinVersionedKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Single primary key
    /// VersionedKV JOIN VersionedKV
    /// versioned_kv(t1) inner all join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::VersionedKV,
        /*left_primary_key_column_indexes*/ std::vector<size_t>{0},
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {{
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/ ExpectedJoinResults{},
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .retracted_values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)(2, '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .retracted_values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:01', +1)(2, '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:01', '2023-1-1 00:00:01', 1)"
                           "(2, '2023-1-1 00:00:01', '2023-1-1 00:00:01', 1)",
             },
         }},
        context);
}

TEST(StreamingHashJoin, VersionedKVInnerAllJoinVersionedKVWithPartialMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on partial primary key(s)
    /// VersionedKV JOIN VersionedKV
    /// versioned_kv(t1) inner all join versioned_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::VersionedKV,
        /*left_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {{
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)(1, 't2', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/ ExpectedJoinResults{},
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 't1', '2023-1-1 00:00:00', 1)"
                           "(1, 't1', '2023-1-1 00:00:00', 't2', '2023-1-1 00:00:00', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, 't2', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 't2', '2023-1-1 00:00:00', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 'tt2', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 'tt1', '2023-1-1 00:00:01', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 't1', '2023-1-1 00:00:00', -1)"
                                     "(1, 't1', '2023-1-1 00:00:00', 'tt1', '2023-1-1 00:00:01', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/
             prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 'tt2', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .values = "(1, 'tt1', '2023-1-1 00:00:01', 't1', '2023-1-1 00:00:00', 1)"
                           "(1, 'tt1', '2023-1-1 00:00:01', 'tt1', '2023-1-1 00:00:01', 1)"
                           "(2, 'tt2', '2023-1-1 00:00:01', 'tt2', '2023-1-1 00:00:01', 1)",
             },
         }},
        context);
}

TEST(StreamingHashJoin, VersionedKVInnerAllJoinVersionedKVWithNoPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on no primary key(s)
    /// VersionedKV JOIN VersionedKV
    /// versioned_kv(t1) inner all join versioned_kv(t2) on t1.col_2 = t2.col_2
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::VersionedKV,
        /*left_primary_key_column_indexes*/ std::vector<size_t>{0},
        right_header,
        Streaming::StorageSemantic::VersionedKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {{
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)(2, 't1', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/ ExpectedJoinResults{},
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 1, '2023-1-1 00:00:00', 1)"
                           "(1, 't1', '2023-1-1 00:00:00', 2, '2023-1-1 00:00:00', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 1, '2023-1-1 00:00:00', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(22, 't1', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 22, '2023-1-1 00:00:01', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 2, '2023-1-1 00:00:00', -1)"
                                     "(1, 't1', '2023-1-1 00:00:00', 22, '2023-1-1 00:00:01', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/
             prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 't1', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .values = "(1, 'tt1', '2023-1-1 00:00:01', 1, '2023-1-1 00:00:01', 1)"
                           "(2, 't1', '2023-1-1 00:00:01', 2, '2023-1-1 00:00:00', 1)"
                           "(2, 't1', '2023-1-1 00:00:01', 22, '2023-1-1 00:00:01', 1)",
             },
         }},
        context);
}

TEST(StreamingHashJoin, ChangelogKVInnerAllJoinChangelogKV)
{
    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Single primary key
    /// ChangelogKV JOIN ChangelogKV
    /// changelog_kv(t1) inner all join changelog_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*left_primary_key_column_indexes*/ std::vector<size_t>{0},
        right_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {{
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/ ExpectedJoinResults{},
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .retracted_values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)(2, '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .retracted_values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:01', +1)(2, '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:01', '2023-1-1 00:00:01', 1)"
                           "(2, '2023-1-1 00:00:01', '2023-1-1 00:00:01', 1)",
             },
         }},
        context);
}

TEST(StreamingHashJoin, ChangelogKVInnerAllJoinChangelogKVWithPartialMultiPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on partial primary key(s)
    /// ChangelogKV JOIN ChangelogKV
    /// changelog_kv(t1) inner all join changelog_kv(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*left_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        right_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0, 1},
        /*to_join_steps*/
        {{
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)(1, 't2', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/ ExpectedJoinResults{},
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 't1', '2023-1-1 00:00:00', 1)"
                           "(1, 't1', '2023-1-1 00:00:00', 't2', '2023-1-1 00:00:00', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, 't2', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 't2', '2023-1-1 00:00:00', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 'tt2', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 'tt1', '2023-1-1 00:00:01', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 't1', '2023-1-1 00:00:00', -1)"
                                     "(1, 't1', '2023-1-1 00:00:00', 'tt1', '2023-1-1 00:00:01', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/
             prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 'tt2', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_2, t2.col_3, _tp_delta
                 .values = "(1, 'tt1', '2023-1-1 00:00:01', 't1', '2023-1-1 00:00:00', 1)"
                           "(1, 'tt1', '2023-1-1 00:00:01', 'tt1', '2023-1-1 00:00:01', 1)"
                           "(2, 'tt2', '2023-1-1 00:00:01', 'tt2', '2023-1-1 00:00:01', 1)",
             },
         }},
        context);
}

TEST(StreamingHashJoin, ChangelogKVInnerAllJoinChangelogKVWithNoPrimaryKeys)
{
    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "string", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Join on no primary key(s)
    /// ChangelogKV JOIN ChangelogKV
    /// changelog_kv(t1) inner all join changelog_kv(t2) on t1.col_2 = t2.col_2
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_2 = t2.col_2",
        left_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*left_primary_key_column_indexes*/ std::vector<size_t>{0},
        right_header,
        Streaming::StorageSemantic::ChangelogKV,
        /*right_primary_key_column_indexes*/ std::vector<size_t>{0},
        /*to_join_steps*/
        {{
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', +1)(2, 't1', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/ ExpectedJoinResults{},
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 1, '2023-1-1 00:00:00', 1)"
                           "(1, 't1', '2023-1-1 00:00:00', 2, '2023-1-1 00:00:00', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 1, '2023-1-1 00:00:00', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/
             prepareBlockByHeader(right_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(22, 't1', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .values = "(1, 't1', '2023-1-1 00:00:00', 22, '2023-1-1 00:00:01', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, 't1', '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .retracted_values = "(1, 't1', '2023-1-1 00:00:00', 2, '2023-1-1 00:00:00', -1)"
                                     "(1, 't1', '2023-1-1 00:00:00', 22, '2023-1-1 00:00:01', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/
             prepareBlockByHeader(left_header, "(1, 'tt1', '2023-1-1 00:00:01', +1)(2, 't1', '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, col_3, t2.col_1, t2.col_3, _tp_delta
                 .values = "(1, 'tt1', '2023-1-1 00:00:01', 1, '2023-1-1 00:00:01', 1)"
                           "(2, 't1', '2023-1-1 00:00:01', 2, '2023-1-1 00:00:00', 1)"
                           "(2, 't1', '2023-1-1 00:00:01', 22, '2023-1-1 00:00:01', 1)",
             },
         }},
        context);
}

TEST(StreamingHashJoin, ChangelogInnerAllJoinChangelog)
{
    auto context = getContext().context;
    Block left_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);
    Block right_header = prepareBlockWithDelta(/*types*/ {"int", "datetime64(3, 'UTC')", "int8"}, /*no data*/ "", context);

    /// Changelog JOIN Changelog
    /// changelog(t1) inner all join changelog(t2) on t1.col_1 = t2.col_1
    commonTest(
        /*join kind*/ "inner",
        /*join strictness*/ "all",
        /*on_clause*/ "t1.col_1 = t2.col_1",
        left_header,
        Streaming::StorageSemantic::Changelog,
        /*left_primary_key_column_indexes*/ std::nullopt,
        right_header,
        Streaming::StorageSemantic::Changelog,
        /*right_primary_key_column_indexes*/ std::nullopt,
        /*to_join_steps*/
        {{
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/ ExpectedJoinResults{},
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .retracted_values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:00', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::RIGHT,
             /*to join block*/ prepareBlockByHeader(right_header, "(1, '2023-1-1 00:00:01', +1)(2, '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', 1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:00', -1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .retracted_values = "(1, '2023-1-1 00:00:00', '2023-1-1 00:00:01', -1)",
             },
         },
         {
             /*to join pos*/ ToJoinStep::LEFT,
             /*to join block*/ prepareBlockByHeader(left_header, "(1, '2023-1-1 00:00:01', +1)(2, '2023-1-1 00:00:01', +1)", context),
             /*expected join results*/
             ExpectedJoinResults{
                 /// output header: col_1, col_2, t2.col_2, _tp_delta
                 .values = "(1, '2023-1-1 00:00:01', '2023-1-1 00:00:01', 1)"
                           "(2, '2023-1-1 00:00:01', '2023-1-1 00:00:01', 1)",
             },
         }},
        context);
}
