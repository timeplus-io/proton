#pragma once

#include <Common/SipHash.h>

#include <Poco/Net/Context.h>

#include <string>


namespace DB
{

struct SSLConfiguration
{
    using VerificationMode = Poco::Net::Context::VerificationMode;

    std::string ssl_cert_file;
    std::string ssl_key_file;
    std::string ssl_ca_cert_file;
    VerificationMode verification_mode = VerificationMode::VERIFY_RELAXED;

    bool operator==(const SSLConfiguration & rhs) const
    {
        return std::tie(ssl_ca_cert_file, ssl_key_file, ssl_ca_cert_file, verification_mode)
            == std::tie(rhs.ssl_ca_cert_file, rhs.ssl_key_file, rhs.ssl_ca_cert_file, rhs.verification_mode);
    }

    void updateHash(SipHash & s) const
    {
        s.update(ssl_cert_file);
        s.update(ssl_key_file);
        s.update(ssl_ca_cert_file);
        s.update(verification_mode);
    }
};

}
