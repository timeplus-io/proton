#include <Access/LocalApiToken.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AllowedClientHosts.h>
#include <Access/Common/AuthenticationData.h>
#include <Access/GrantedRoles.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/Role.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace
{
const String LOCAL_API_USERNAME = "local_api_user";
const String ROLE_NAME = "local_api_role";
const String MEMORY_STORAGE_NAME = "local_api";

bool g_enabled = false;
String g_token;

String stripHyphens(String s)
{
    s.erase(std::remove(s.begin(), s.end(), '-'), s.end());
    return s;
}

/// Two V4 UUIDs stripped of hyphens — 256-bit entropy, 64 hex chars.
String generateToken()
{
    return stripHyphens(toString(UUIDHelpers::generateV4())) + stripHyphens(toString(UUIDHelpers::generateV4()));
}

std::shared_ptr<MemoryAccessStorage> findMemoryStorage(const String & storage_name, const std::shared_ptr<AccessControl> & access_control)
{
    for (const auto & storage : access_control->getStorages())
    {
        if (storage->getStorageName() == storage_name)
            return std::dynamic_pointer_cast<MemoryAccessStorage>(storage);
    }
    return nullptr;
}

/// Parse a comma-separated list of access type names, e.g. "SELECT, INSERT".
Strings parsePrivileges(const String & privileges_cfg)
{
    Strings keywords;
    boost::split(keywords, privileges_cfg, boost::is_any_of(","));
    for (auto & kw : keywords)
        boost::trim(kw);
    std::erase_if(keywords, [](const String & s) { return s.empty(); });
    return keywords;
}
}


void LocalApiToken::initialize(ContextMutablePtr context)
{
    const auto & cfg = context->getConfigRef();

    if (!cfg.getBool("local_api_user.enabled", true))
        return;

    /// Parse comma-separated access types, e.g. "SELECT" or "SELECT, INSERT".
    const String privileges_cfg = cfg.getString("local_api_user.privileges", "SELECT,INSERT,CREATE TEMPORARY TABLE");
    const Strings privileges = parsePrivileges(privileges_cfg);
    if (privileges.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "local_api_user.privileges must not be empty");

    const AccessFlags access_flags(privileges);

    g_token = generateToken();

    auto access_control = context->getAccessControl();

    /// Ensure a MemoryAccessStorage exists so that the user and role are
    /// never written to disk.  addMemoryStorage() is a no-op if one already
    /// exists, so this is safe to call unconditionally.
    access_control->addMemoryStorage(MEMORY_STORAGE_NAME);

    auto mem_storage = findMemoryStorage(MEMORY_STORAGE_NAME, access_control);
    if (!mem_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to locate MemoryAccessStorage for local API user");

    /// Create the role and grant exactly the configured access flags.
    auto role = std::make_shared<Role>();
    role->setName(ROLE_NAME);
    role->access.grantWithGrantOption(access_flags);

    /// insert(replace=true, throw=false) is idempotent: safe if server is
    /// reloaded or initialize() is ever called more than once.
    auto role_id_opt = mem_storage->insert(role, /*replace_if_exists=*/true, /*throw_if_exists=*/false);
    if (!role_id_opt)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to insert local API role '{}'", ROLE_NAME);
    const UUID role_id = *role_id_opt;

    /// Create the user, restricted to loopback addresses.
    auto user = std::make_shared<User>();
    user->setName(LOCAL_API_USERNAME);
    user->auth_data = AuthenticationData{AuthenticationType::PLAINTEXT_PASSWORD};
    user->auth_data.setPassword(g_token);

    /// Restrict to IPv4 and IPv6 loopback — non-local connections are
    /// rejected at the AccessControl layer even before our handler guards.
    user->allowed_client_hosts.addAddress("127.0.0.1");
    user->allowed_client_hosts.addAddress("::1");

    user->granted_roles.grant(role_id);

    /// Make all granted roles active by default.
    user->default_roles.all = true;

    mem_storage->insert(user, /*replace_if_exists=*/true, /*throw_if_exists=*/false);

    g_enabled = true;
}


bool LocalApiToken::isEnabled()
{
    return g_enabled;
}

bool LocalApiToken::isLocalApiTokenUser(const String & user)
{
    return g_enabled && user == LOCAL_API_USERNAME;
}

const String & LocalApiToken::username()
{
    return LOCAL_API_USERNAME;
}

const String & LocalApiToken::token()
{
    return g_token;
}

}
