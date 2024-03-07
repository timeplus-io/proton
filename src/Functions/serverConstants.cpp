#include <Functions/FunctionConstantBase.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Core/ServerUUID.h>
#include <Common/SymbolIndex.h>
#include <Common/DNSResolver.h>
#include <Common/DateLUT.h>
#include <Common/VersionRevision.h>

#include <Poco/Environment.h>

#include "config_version.h"


namespace DB
{
namespace
{

#if defined(__ELF__) && !defined(OS_FREEBSD)
    /// buildId() - returns the compiler build id of the running binary.
    class FunctionBuildId : public FunctionConstantBase<FunctionBuildId, String, DataTypeString>
    {
    public:
        static constexpr auto name = "build_id";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionBuildId>(context); }
        explicit FunctionBuildId(ContextPtr context) : FunctionConstantBase(SymbolIndex::instance().getBuildIDHex(), context->isDistributed()) {}
    };
#endif


    /// Get the host name. Is is constant on single server, but is not constant in distributed queries.
    class FunctionHostName : public FunctionConstantBase<FunctionHostName, String, DataTypeString>
    {
    public:
        static constexpr auto name = "hostname";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionHostName>(context); }
        explicit FunctionHostName(ContextPtr context) : FunctionConstantBase(DNSResolver::instance().getHostName(), context->isDistributed()) {}
    };


    class FunctionServerUUID : public FunctionConstantBase<FunctionServerUUID, UUID, DataTypeUUID>
    {
    public:
        static constexpr auto name = "server_uuid";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionServerUUID>(context); }
        explicit FunctionServerUUID(ContextPtr context) : FunctionConstantBase(ServerUUID::get(), context->isDistributed()) {}
    };


    class FunctionTcpPort : public FunctionConstantBase<FunctionTcpPort, UInt16, DataTypeUInt16>
    {
    public:
        static constexpr auto name = "tcp_port";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTcpPort>(context); }
        explicit FunctionTcpPort(ContextPtr context) : FunctionConstantBase(context->getTCPPort(), context->isDistributed()) {}
    };


    /// Returns the server time zone.
    class FunctionTimezone : public FunctionConstantBase<FunctionTimezone, String, DataTypeString>
    {
    public:
        static constexpr auto name = "timezone";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimezone>(context); }
        explicit FunctionTimezone(ContextPtr context) : FunctionConstantBase(String{DateLUT::instance().getTimeZone()}, context->isDistributed()) {}
    };


    /// Returns server uptime in seconds.
    class FunctionUptime : public FunctionConstantBase<FunctionUptime, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "uptime";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionUptime>(context); }
        explicit FunctionUptime(ContextPtr context) : FunctionConstantBase(context->getUptimeSeconds(), context->isDistributed()) {}
    };


    /// version() - returns the current version as a string.
    class FunctionVersion : public FunctionConstantBase<FunctionVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "version";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionVersion>(context); }
        explicit FunctionVersion(ContextPtr context) : FunctionConstantBase(VERSION_STRING, context->isDistributed()) {}
    };

    /// revision() - returns the current revision.
    class FunctionRevision : public FunctionConstantBase<FunctionRevision, UInt32, DataTypeUInt32>
    {
    public:
        static constexpr auto name = "revision";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionRevision>(context); }
        explicit FunctionRevision(ContextPtr context) : FunctionConstantBase(ProtonRevision::getVersionRevision(), context->isDistributed()) {}
    };

    /// edition() - returns the current edition.
    class FunctionEdition : public FunctionConstantBase<FunctionEdition, String, DataTypeString>
    {
    public:
        static constexpr auto name = "edition";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionEdition>(context); }
        explicit FunctionEdition(ContextPtr context) : FunctionConstantBase(EDITION, context->isDistributed()) {}
    };

    class FunctionGetOSKernelVersion : public FunctionConstantBase<FunctionGetOSKernelVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "get_os_kernel_version";
        explicit FunctionGetOSKernelVersion(ContextPtr context) : FunctionConstantBase(Poco::Environment::osName() + " " + Poco::Environment::osVersion(), context->isDistributed()) {}
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGetOSKernelVersion>(context); }
    };

    class FunctionDisplayName : public FunctionConstantBase<FunctionDisplayName, String, DataTypeString>
    {
    public:
        static constexpr auto name = "display_name";
        explicit FunctionDisplayName(ContextPtr context) : FunctionConstantBase(context->getConfigRef().getString("display_name", getFQDNOrHostName()), context->isDistributed()) {}
        static FunctionPtr create(ContextPtr context) {return std::make_shared<FunctionDisplayName>(context); }
    };
}

#if defined(__ELF__) && !defined(OS_FREEBSD)
REGISTER_FUNCTION(BuildId)
{
    factory.registerFunction<FunctionBuildId>();
}
#endif

REGISTER_FUNCTION(HostName)
{
    factory.registerFunction<FunctionHostName>();
}

REGISTER_FUNCTION(ServerUUID)
{
    factory.registerFunction<FunctionServerUUID>();
}

REGISTER_FUNCTION(TcpPort)
{
    factory.registerFunction<FunctionTcpPort>();
}

REGISTER_FUNCTION(Timezone)
{
    factory.registerFunction<FunctionTimezone>();
}

REGISTER_FUNCTION(Uptime)
{
    factory.registerFunction<FunctionUptime>();
}

REGISTER_FUNCTION(Version)
{
    factory.registerFunction<FunctionVersion>({}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(Revision)
{
    factory.registerFunction<FunctionRevision>({}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(Edition)
{
    factory.registerFunction<FunctionEdition>({}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(GetOSKernelVersion)
{
    factory.registerFunction<FunctionGetOSKernelVersion>();
}


REGISTER_FUNCTION(DisplayName)
{
    factory.registerFunction<FunctionDisplayName>(
        {
            R"(
Returns the value of `display_name` from config or server FQDN if not set.

[example:displayName]
)",
            Documentation::Examples{{"displayName", "SELECT display_name();"}},
            Documentation::Categories{"Constant", "Miscellaneous"}
        },
        FunctionFactory::CaseSensitive);
}


}
