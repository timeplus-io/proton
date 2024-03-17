#include <Storages/ExternalTable/ExternalTableFactory.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
}

void registerClickHouseExternalTable(ExternalTableFactory & factory);

ExternalTableFactory & ExternalTableFactory::instance()
{
    static DB::ExternalTableFactory ret;
    return ret;
}

void ExternalTableFactory::registerExternalTable(const String & type, Creator creator)
{
    if (creators.contains(type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ExternalTableFactory: type {} is already registered", type);

    creators[type] = std::move(creator);
}

IExternalTablePtr ExternalTableFactory::getExternalTable(const String & name, ExternalTableSettingsPtr settings) const
{
    auto type = settings->type.value;
    if (!creators.contains(type))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown external table type {}", type);

    return creators.at(type)(name, std::move(settings));
}

ExternalTableFactory::ExternalTableFactory()
{
    registerClickHouseExternalTable(*this);
}

}
