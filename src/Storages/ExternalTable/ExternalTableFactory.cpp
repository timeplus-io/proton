#include <Storages/ExternalTable/ExternalTableFactory.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
}

ExternalTableFactory & ExternalTableFactory::instance()
{
    static DB::ExternalTableFactory ret;
    return ret;
}

void ExternalTableFactory::registerExternalTable(const std::string & type, Creator creator)
{
    if (creators.contains(type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ExternalTableFactory: type {} is already registered", type);

    creators[type] = std::move(creator);
}

IExternalTablePtr ExternalTableFactory::getExternalTable(const std::string & type, ExternalTableSettingsPtr settings) const
{
    if (!creators.contains(type))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown external table type {}", type);

    return creators.at(type)(std::move(settings));
}

}
