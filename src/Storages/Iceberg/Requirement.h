#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace Poco::JSON
{
class Array;
}

namespace Apache::Iceberg
{

class Requirement
{
public:
    virtual ~Requirement() = default;

    virtual void apply(Poco::JSON::Array & requirements) const = 0;
};

class AssertRefSnapshotID : public Requirement
{
public:
    AssertRefSnapshotID(const std::string & ref_, int64_t snapshot_id_) : ref(ref_), snapshot_id(snapshot_id_) { }
    ~AssertRefSnapshotID() override = default;

    void apply(Poco::JSON::Array & requirements) const override;

private:
    const std::string ref;
    const int64_t snapshot_id{-1};
};

class AssertTableUUID : public Requirement
{
public:
    explicit AssertTableUUID(const std::string & uuid_) : uuid(uuid_) { }
    ~AssertTableUUID() override = default;

    void apply(Poco::JSON::Array & requirements) const override;

private:
    const std::string uuid;
};

using RequirementPtr = std::unique_ptr<Requirement>;
using Requirements = std::vector<RequirementPtr>;

}
