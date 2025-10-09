#pragma once

#include "config.h"
#if USE_PROTOBUF

#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <boost/noncopyable.hpp>

/// proton: starts
#include <Formats/KafkaSchemaRegistry.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/tokenizer.h>
/// proton: ends


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

/** Keeps parsed google protobuf schemas parsed from files.
  * This class is used to handle the "Protobuf" input/output formats.
  */
class ProtobufSchemas : public google::protobuf::io::ErrorCollector /* proton: updated */
{
public:
    enum class WithEnvelope
    {
        // Return descriptor for a top-level message with a user-provided name.
        // Example: In protobuf schema
        //   message MessageType {
        //     string colA = 1;
        //     int32 colB = 2;
        //   }
        // message_name = "MessageType" returns a descriptor. Used by IO
        // formats Protobuf and ProtobufSingle.
        No,
        // Return descriptor for a message with a user-provided name one level
        // below a top-level message with the hardcoded name "Envelope".
        // Example: In protobuf schema
        //   message Envelope {
        //     message MessageType {
        //       string colA = 1;
        //       int32 colB = 2;
        //     }
        //   }
        // message_name = "MessageType" returns a descriptor. Used by IO format
        // ProtobufList.
        Yes
    };

    static ProtobufSchemas & instance();
    // Clear cached protobuf schemas
    void clear();

    class ImporterWithSourceTree;
    struct DescriptorHolder
    {
        DescriptorHolder(std::shared_ptr<ImporterWithSourceTree> importer_, const google::protobuf::Descriptor * message_descriptor_)
            : importer(std::move(importer_))
            , message_descriptor(message_descriptor_)
        {}
    private:
        std::shared_ptr<ImporterWithSourceTree> importer;
    public:
        const google::protobuf::Descriptor * message_descriptor;
    };

    /// Parses the format schema, then parses the corresponding proto file, and
    /// returns holder (since the descriptor only valid if
    /// ImporterWithSourceTree is valid):
    ///
    ///     {ImporterWithSourceTree, protobuf::Descriptor - descriptor of the message type}.
    ///
    /// The function always return valid message descriptor, it throws an exception if it cannot load or parse the file.
    DescriptorHolder
    getMessageTypeForFormatSchema(const FormatSchemaInfo & info, WithEnvelope with_envelope, const String & google_protos_path);

    /// proton: starts
    void AddError(int line, google::protobuf::io::ColumnNumber column, const std::string & message) override;

    /// Validates the given schema and throw a DB::Exception if the schema is invalid.
    /// The exception will contain the first error encountered when validating the schema, i.e. there could be more errors.
    void validateSchema(std::string_view schema);
    /// proton: ends
private:
    std::unordered_map<String, std::shared_ptr<ImporterWithSourceTree>> importers;
    std::mutex mutex;
};

}

#endif
