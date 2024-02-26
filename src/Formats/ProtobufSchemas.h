#pragma once

#include "config.h"
#if USE_PROTOBUF

#include <Formats/KafkaSchemaRegistry.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/tokenizer.h>


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
class ProtobufSchemas : private boost::noncopyable, public google::protobuf::io::ErrorCollector /* proton: updated */
{
public:
    static ProtobufSchemas & instance();

    ProtobufSchemas();
    ~ProtobufSchemas() override; /* proton: updated */

    /// Parses the format schema, then parses the corresponding proto file, and returns the descriptor of the message type.
    /// The function never returns nullptr, it throws an exception if it cannot load or parse the file.
    const google::protobuf::Descriptor * getMessageTypeForFormatSchema(const FormatSchemaInfo & info);

    /// proton: starts
    void AddError(int line, google::protobuf::io::ColumnNumber column, const std::string & message) override;

    /// Validates the given schema and throw a DB::Exception if the schema is invalid.
    /// The exception will contain the first error encountered when validating the schema, i.e. there could be more errors.
    void validateSchema(std::string_view schema);
    /// proton: ends
private:
    class ImporterWithSourceTree;
    std::unordered_map<String, std::unique_ptr<ImporterWithSourceTree>> importers;
    std::mutex mutex;
};

}

#endif
