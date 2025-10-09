#include <Storages/Iceberg/Requirement.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <cassert>

namespace Apache::Iceberg
{

void AssertRefSnapshotID::apply(Poco::JSON::Array & requirements) const
{
    assert(!ref.empty());

    Poco::JSON::Object obj;
    obj.set("type", "assert-ref-snapshot-id");
    obj.set("ref", ref);
    if (snapshot_id > 0)
        obj.set("snapshot-id", snapshot_id);
    requirements.add(obj);
}

void AssertTableUUID::apply(Poco::JSON::Array & requirements) const
{
    assert(!uuid.empty());

    Poco::JSON::Object obj;
    obj.set("type", "assert-table-uuid");
    obj.set("uuid", uuid);
    requirements.add(obj);
}

}
