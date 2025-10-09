#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/IntervalKind.h>


namespace DB
{

class ASTLiteral;

class ASTCreateTaskQuery : public IAST
{
public:
    ASTPtr database;
    ASTPtr name;

    bool or_replace = false;
    bool if_not_exists = false;

    String cron;

    UInt64 interval{0};
    IntervalKind interval_unit;

    UInt64 timeout{1};
    IntervalKind timeout_unit{IntervalKind::Kind::Hour};

    String checkpoint;
    std::vector<String> checkpoint_names;

    StorageID to_table_id = StorageID::createEmpty();

    String sql;

    String getID(char delim) const override { return "CreateTaskQuery" + (delim + getName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    String getDatabase() const;
    String getName() const;

    // String getSelectQuery() const;
};

}
