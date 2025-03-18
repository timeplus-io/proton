#pragma once

#include <Core/Types.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Apache::Iceberg
{

class IStorageCredentials
{
public:
    virtual ~IStorageCredentials() = default;

    virtual void addCredentialsToSettings(DB::ASTSetQuery & settings) const = 0;
};

class S3Credentials final : public IStorageCredentials
{
public:
    /// TODO: support region as well.
    S3Credentials(const std::string & access_key_id_, const std::string & secret_access_key_, const std::string session_token_)
        : access_key_id(access_key_id_), secret_access_key(secret_access_key_), session_token(session_token_)
    {
    }

    void addCredentialsToSettings(DB::ASTSetQuery & settings) const override
    {
        settings.changes.setSetting("access_key_id", access_key_id);
        settings.changes.setSetting("secret_access_key", secret_access_key);
        settings.changes.setSetting("session_token", session_token);
    }

private:
    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
};

}
