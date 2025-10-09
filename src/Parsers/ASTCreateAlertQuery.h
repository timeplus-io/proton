#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/IntervalKind.h>

namespace DB
{

class ASTCreateAlertQuery : public IAST
{
public:
    ASTPtr database;
    ASTPtr name;
    ASTSelectQuery * select{nullptr};

    String function_name;

    UInt64 batch_size{0};
    UInt64 batch_timeout{0};
    IntervalKind batch_timeout_unit;

    UInt64 limit_count{0};
    UInt64 limit_interval{0};
    IntervalKind limit_interval_kind;

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateAlertQuery" + (delim + getName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    String getDatabase() const;
    String getName() const;

    String getSelectQuery() const;
};

}
