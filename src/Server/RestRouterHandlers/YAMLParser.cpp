#include "YAMLParser.h"

#include <yaml-cpp/yaml.h> // Y_IGNORE

#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_YAML;
}

bool yamlNodeToJSONNode(const YAML::Node & ynode, Poco::Dynamic::Var & jnode)
{
    switch (ynode.Type())
    {
        case YAML::NodeType::Scalar: {
            Poco::Dynamic::Var v;
            if (ynode.Scalar() == "false" || ynode.Scalar() == "true")
            {
                v = {ynode.as<bool>()};
            }
            else
            {
                v = {ynode.as<std::string>()};
            }

            jnode.swap(v);
            return true;
        }

        case YAML::NodeType::Sequence: {
            Poco::JSON::Array arry;
            for (const auto & child_node : ynode)
            {
                Poco::Dynamic::Var v;
                if (yamlNodeToJSONNode(child_node, v))
                {
                    arry.add(v);
                }
                else
                {
                    return false;
                }
            }

            jnode = arry;
            return true;
        }

        case YAML::NodeType::Map: {
            Poco::JSON::Object obj;
            for (const auto key_value_pair : ynode)
            {
                Poco::Dynamic::Var v;
                if (yamlNodeToJSONNode(key_value_pair.second, v))
                {
                    obj.set(key_value_pair.first.Scalar(), v);
                }
                else
                {
                    return false;
                }
            }

            jnode = obj;
            return true;
        }

        case YAML::NodeType::Null:
            return false;

        case YAML::NodeType::Undefined: {
            throw Exception(
                ErrorCodes::CANNOT_PARSE_YAML,
                "YAMLParser has encountered node with undefined type and cannot continue parsing of the file");
        }
    }
    return true;
}

Poco::JSON::Object parseToJson(const String & path)
{
    YAML::Node node_yml;
    try
    {
        node_yml = YAML::LoadFile(path);
    }
    catch (const YAML::ParserException & e)
    {
        /// yaml-cpp cannot parse the file because its contents are incorrect
        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "Unable to parse YAML configuration file {}", path, e.what());
    }
    catch (const YAML::BadFile &)
    {
        /// yaml-cpp cannot open the file even though it exists
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Unable to open YAML configuration file {}", path);
    }

    Poco::Dynamic::Var value;
    yamlNodeToJSONNode(node_yml, value);

    return value.extract<Poco::JSON::Object>();
}

}
