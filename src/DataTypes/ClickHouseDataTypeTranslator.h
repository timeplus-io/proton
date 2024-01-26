#pragma once

#include <DataTypes/IDataTypeTranslator.h>

namespace DB
{

class ClickHouseDataTypeTranslator final : public IDataTypeTranslator
{
public:
    static ClickHouseDataTypeTranslator & instance();

    ~ClickHouseDataTypeTranslator() override = default;

    std::string translate(const std::string & type_name) override;

private:
    ClickHouseDataTypeTranslator();

    std::unordered_map<std::string, std::string> type_dict;
};

}
