#include "config.h"

#if USE_PROTOBUF

#include <Processors/Formats/Impl/ProtobufRowInputFormat.h>

#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// Records parse errors instead of throwing, so a test can assert success/failure.
class CollectingErrorCollector : public google::protobuf::io::ErrorCollector
{
public:
    void AddError(int line, google::protobuf::io::ColumnNumber column, const std::string & message) override
    {
        errors.emplace_back(fmt::format("line={} column={} {}", line, column, message));
    }
    std::vector<std::string> errors;
};

/// Mirror of ProtobufConfluentRowInputFormat::fetchSchema's parse+build step:
/// returns true if the schema TEXT both parses and builds into a FileDescriptor.
bool parsesAndBuilds(const std::string & schema, std::string & error_out)
{
    CollectingErrorCollector collector;
    google::protobuf::io::ArrayInputStream input{schema.data(), static_cast<int>(schema.size())};
    google::protobuf::io::Tokenizer tokenizer(&input, &collector);
    google::protobuf::FileDescriptorProto file_descriptor;
    file_descriptor.set_name("test_schema");
    google::protobuf::compiler::Parser parser;
    parser.RecordErrorsTo(&collector);
    parser.Parse(&tokenizer, &file_descriptor);

    if (!collector.errors.empty())
    {
        error_out = collector.errors.front();
        return false;
    }

    google::protobuf::DescriptorPool pool;
    const auto * descriptor = pool.BuildFile(file_descriptor);
    return descriptor != nullptr && descriptor->message_type_count() > 0;
}

/// The real registry-stored shape (descriptor-canonicalized): no `syntax`, fully-qualified
/// names, maps expanded into synthetic *Entry messages with `option map_entry = true`.
/// Includes a uint32->uint32 map and a uint32->enum map.
const std::string kExpandedSchema = R"PROTO(
package GK.metrics.proto;

message FrameByFrameMetrics {
  optional uint64 baseTimeStampForEvent = 2;
  repeated .GK.metrics.proto.FrameByFrameMetrics.CongestionControlStateEntry congestionControlState = 38;
  repeated .GK.metrics.proto.FrameByFrameMetrics.CliDscpSentReportsEntry cliDscpSentReports = 81;
  optional uint32 cliEcnCeCount = 85;

  message CongestionControlStateEntry {
    option map_entry = true;

    optional uint32 key = 1;
    optional .GK.metrics.proto.CongestionControlState value = 2;
  }
  message CliDscpSentReportsEntry {
    option map_entry = true;

    optional uint32 key = 1;
    optional uint32 value = 2;
  }
}
enum CongestionControlState {
  CongestionControlState_NOTSET = 0;
  CongestionControlState_STABLE = 1;
}
)PROTO";

}

/// The expanded form must not parse as-is (this is the bug we mitigate)...
TEST(ProtobufMapEntry, ExpandedFormFailsToParseAsIs)
{
    std::string err;
    EXPECT_FALSE(parsesAndBuilds(kExpandedSchema, err));
    EXPECT_NE(err.find("map_entry"), std::string::npos) << "unexpected error: " << err;
}

/// ...and the collapsed form must parse and build cleanly.
TEST(ProtobufMapEntry, CollapsedFormParsesAndBuilds)
{
    const std::string collapsed = collapseProtobufMapEntries(kExpandedSchema);

    EXPECT_EQ(collapsed.find("map_entry"), std::string::npos) << "map_entry option not removed";
    EXPECT_NE(collapsed.find("map<uint32, uint32> cliDscpSentReports = 81;"), std::string::npos);
    EXPECT_NE(collapsed.find("map<uint32, .GK.metrics.proto.CongestionControlState> congestionControlState = 38;"),
              std::string::npos);

    /// The collapsed schema lacks `syntax`, which defaults to proto2 -- still must build.
    std::string err;
    EXPECT_TRUE(parsesAndBuilds(collapsed, err)) << "collapsed schema failed: " << err;
}

/// No-op safety: a schema without any map_entry option is returned byte-for-byte unchanged.
TEST(ProtobufMapEntry, NoOpWithoutMapEntry)
{
    const std::string plain = R"PROTO(
syntax = "proto2";
message M {
  optional uint64 ts = 1;
  repeated uint32 values = 2 [packed = true];
}
)PROTO";
    EXPECT_EQ(collapseProtobufMapEntries(plain), plain);
}

/// No-op safety: a schema already using map<> sugar (no map_entry text) is unchanged.
TEST(ProtobufMapEntry, NoOpWithMapSugar)
{
    const std::string sugar = R"PROTO(
syntax = "proto3";
message M {
  map<uint32, uint32> counts = 1;
}
)PROTO";
    EXPECT_EQ(collapseProtobufMapEntries(sugar), sugar);
}

#endif
