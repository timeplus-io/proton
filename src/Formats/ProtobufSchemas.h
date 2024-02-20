#pragma once

#include "config.h"
#if USE_PROTOBUF

#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <google/protobuf/descriptor.h>


namespace google
{
namespace protobuf
{
    class Descriptor;
}
}

namespace DB
{
class FormatSchemaInfo;
/// proton: starts
struct SchemaValidationError;
using SchemaValidationErrors = std::vector<SchemaValidationError>;
/// proton: ends

/** Keeps parsed google protobuf schemas parsed from files.
  * This class is used to handle the "Protobuf" input/output formats.
  */
class ProtobufSchemas : private boost::noncopyable
{
public:
    static ProtobufSchemas & instance();

    ProtobufSchemas();
    ~ProtobufSchemas();

    /// Parses the format schema, then parses the corresponding proto file, and returns the descriptor of the message type.
    /// The function never returns nullptr, it throws an exception if it cannot load or parse the file.
    const google::protobuf::Descriptor * getMessageTypeForFormatSchema(const FormatSchemaInfo & info);

    /// proton: starts
    class SchemaRegistry;

    /// Fetches the schema from the (Kafka) schema registry and returns the descriptor of the first message type.
    const google::protobuf::Descriptor * getMessageTypeForSchemaRegistry(const String & base_url, UInt32 schema_id);
    /// Fetches the schema from the (Kafka) schema registry and returns the descriptor of the message type indicated by the indexes.
    const google::protobuf::Descriptor * getMessageTypeForSchemaRegistry(const String & base_url, UInt32 schema_id, const std::vector<Int64> & indexes);

    SchemaValidationErrors validateSchema(std::string_view schema);
    /// proton: ends
private:
    class ImporterWithSourceTree;
    std::unordered_map<String, std::unique_ptr<ImporterWithSourceTree>> importers;
    std::mutex mutex;

    /// proton: starts
    google::protobuf::DescriptorPool registry_pool_;

    google::protobuf::DescriptorPool* registry_pool() { return &registry_pool_; }
    /// proton: ends
};

}

#endif
