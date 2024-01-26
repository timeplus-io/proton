#pragma once

namespace DB
{

class IDataTypeTranslator
{
public:
    virtual ~IDataTypeTranslator() = default;

    virtual std::string translate(const std::string & type_name) = 0;
};

}
