#pragma once

#include <base/types.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
/// Manages the ephemeral local API user used by Python UDFs and other
/// in-process callers to authenticate back to the database engine over
/// the native TCP protocol or the REST HTTP interface.
///
/// The user and its token live in MemoryAccessStorage only — they are
/// never written to disk and are regenerated on every server restart.
///
/// Configuration (config.yaml):
///
///   local_api_user:
///       enabled: true          # default true; set false to disable entirely
///       privileges: SELECT     # comma-separated GRANT privileges, e.g. "SELECT, INSERT"
///
/// Usage from a Python UDF callback:
///
///   # TCP
///   proton client -h 127.0.0.1 \
///       --user local_api_user --password <token>
///
///   # HTTP (reuse existing headers)
///   curl http://127.0.0.1:3218/... \
///       -H "x-timeplus-user: local_api_user" \
///       -H "x-timeplus-key: <token>"
class LocalApiToken
{
public:
    /// Called once at server startup, after AccessControl is fully set up.
    /// Reads config, generates the token, and creates the user + role in
    /// MemoryAccessStorage.  Safe to call multiple times (idempotent).
    static void initialize(ContextMutablePtr context);

    /// Returns true after a successful initialize() call.
    static bool isEnabled();

    /// The fixed username for the local API user.
    static const String & username();

    /// Returns true when the local API token is enabled and the given user is
    /// the local API user.  Use this instead of comparing against username()
    /// directly so that the enabled check is never forgotten.
    static bool isLocalApiTokenUser(const String & user);

    /// The generated token (serves as the password).
    /// NEVER pass this value to any LOG_ macro.
    static const String & token();
};

}
