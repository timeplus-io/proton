#include <Disks/StorageHelpers.h>

#include <Cluster/Protocol/DiskDescriptor.h>
#include <Cluster/Protocol/StoragePolicyDescriptor.h>

#include <Disks/getDiskConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateDiskQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Common/Config/YAMLParser.h>
#include <Common/StringUtils/StringUtils.h>

#include <fmt/core.h>
#include <magic_enum.hpp>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/Text.h>
#include <Poco/Util/XMLConfiguration.h>

#include <algorithm>
#include <cctype>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
cluster::protocol::DiskDescriptor::DiskType parseDiskType(const StoragePolicyConfigurationPtr & config, const String & config_prefix)
{
    String type = config->getString(config_prefix + ".type", "unknown");

    if (type == "unknown")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'type' when create disk");

    cluster::protocol::DiskDescriptor::DiskType disk_type = cluster::protocol::DiskDescriptor::DiskType::Unknown;
    if (type == "local")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::Local;
    else if (type == "encrypted")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::Encrypted;
    else if (type == "cache")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::Cache;
    else if (type == "s3")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::S3;
    else if (type == "s3_plain")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::S3Plain;
    else if (type == "s3_plain_rewritable")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::S3Plain;
    else if (type == "web")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::Web;
    else if (type == "azure_blob_storage")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::AzureBlobStorage;
    else if (type == "local_blob_storage")
        disk_type = cluster::protocol::DiskDescriptor::DiskType::LocalBlobStorage;

    if (disk_type == cluster::protocol::DiskDescriptor::DiskType::Unknown)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk type '{}' is not supported.", type);

    return disk_type;
}

cluster::protocol::VolumeDescriptor::VolumeLoadBalancing parseVolumeLoadBalancing(const String & config)
{
    if (config == "round_robin")
        return cluster::protocol::VolumeDescriptor::VolumeLoadBalancing::RoundRobin;
    if (config == "least_used")
        return cluster::protocol::VolumeDescriptor::VolumeLoadBalancing::LeastUsed;
    if (config == "unknown")
        return cluster::protocol::VolumeDescriptor::VolumeLoadBalancing::Unknown;
    throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "'{}' is not valid load_balancing value", config);
}
}

cluster::protocol::DiskDescriptorPtr getDiskDescriptorFromAST(ASTCreateDiskQuery & create_disk_query, ContextPtr context)
{
    auto disk_desc = std::make_shared<cluster::protocol::DiskDescriptor>();
    auto disk_name = create_disk_query.name;

    disk_desc->name = disk_name;
    disk_desc->config = serializeAST(*create_disk_query.disk_func, true);

    const auto & ast_function = assert_cast<const ASTFunction &>(*create_disk_query.disk_func);
    const auto * function_args_expr = assert_cast<const ASTExpressionList *>(ast_function.arguments.get());
    const auto & function_args = function_args_expr->children;
    auto config = getDiskConfigurationFromAST(disk_name, function_args, context);

    if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Disk name can contain only alphanumeric and '_' ({})", disk_name);

    disk_desc->type = parseDiskType(config, disk_name);

    return disk_desc;
}

StoragePolicyConfigurationPtr getStoragePolicyConfigurationFromYaml(const std::string & policy_name, const std::string & yaml_str)
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> conf(new Poco::Util::XMLConfiguration());
    Poco::AutoPtr<Poco::XML::Document> xml_document = YAMLParser::parseContent(yaml_str);

    // Get the root element (parent element)
    auto * root = xml_document->documentElement();
    Poco::AutoPtr<Poco::XML::Element> policy_elem(xml_document->createElement(policy_name));

    Poco::XML::NodeList * children = root->childNodes();
    bool has_volumes = false;
    while (children->length() > 0)
    {
        Poco::XML::Node * child = children->item(0); // Always take the first child
        if (child->nodeName() == "volumes")
            has_volumes = true;
        root->removeChild(child);
        policy_elem->appendChild(child);
    }

    if (!has_volumes)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Invalid storage policy yaml configuration, missing 'volumes' node");

    root->appendChild(policy_elem);

    conf->load(xml_document);
    return conf;
}

cluster::protocol::StoragePolicyDescriptorPtr getStoragePolicyDescriptorFromConfiguration(
    const std::string & policy_name, const StoragePolicyConfigurationPtr & config, ContextPtr & context)
{
    auto desc = std::make_shared<cluster::protocol::StoragePolicyDescriptor>();
    desc->name = policy_name;
    desc->move_factor = config->getDouble(policy_name + ".move_factor", 0.1);

    /// check volumes
    String volumes_prefix = policy_name + ".volumes";

    if (!config->has(volumes_prefix))
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Storage policy {} must contain at least one volume (.volumes)", backQuote(policy_name));

    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys(volumes_prefix, keys);

    auto disk_map = context->getDisksMap();
    auto check_and_create_volume = [&](const String & volume_key, cluster::protocol::VolumeDescriptor & target_desc) {
        target_desc.max_data_part_size = config->getUInt64(volume_key + ".max_data_part_size_bytes", 0);
        target_desc.max_data_part_size_ratio = config->getDouble(volume_key + ".max_data_part_size_ratio", 0);
        target_desc.perform_ttl_move_on_insert = config->getBool(volume_key + ".perform_ttl_move_on_insert", true);
        target_desc.prefer_not_to_merge = config->getBool(volume_key + ".prefer_not_to_merge", false);
        target_desc.volume_priority = config->getUInt64(volume_key + ".volume_priority", std::numeric_limits<UInt64>::max());
        target_desc.least_used_ttl_ms = config->getUInt64(volume_key + ".least_used_ttl_ms", 60'000);
        String load_balancing_str = config->getString(volume_key + ".load_balancing", "unknown");
        target_desc.load_balancing = parseVolumeLoadBalancing(load_balancing_str);

        Poco::Util::AbstractConfiguration::Keys disk_keys;
        config->keys(volume_key, disk_keys);
        for (const auto & disk : disk_keys)
        {
            if (startsWith(disk, "disk"))
            {
                auto disk_name = config->getString(volume_key + "." + disk);
                if (!disk_map.contains(disk_name))
                    throw Exception(
                        ErrorCodes::INVALID_CONFIG_PARAMETER, "Disk '{}' in volume '{}' does not exists.", disk_name, target_desc.name);
                target_desc.disk_names.push_back(disk_name);
            }
        }

        if (target_desc.disk_names.empty())
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Volume {} must contain at least one disk.", target_desc.name);
    };

    for (const auto & vol_name : keys)
    {
        if (!std::all_of(vol_name.begin(), vol_name.end(), isWordCharASCII))
            throw Exception(
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                "Volume name can contain only alphanumeric and '_' in storage policy {} ({})",
                backQuote(policy_name),
                vol_name);

        cluster::protocol::VolumeDescriptor volume_desc;
        volume_desc.name = vol_name;
        check_and_create_volume(volumes_prefix + "." + vol_name, volume_desc);
        desc->volumes.emplace_back(std::move(volume_desc));
    }

    return desc;
}

StoragePolicyConfigurationPtr getStoragePolicyConfigurationFromDescriptor(const cluster::protocol::StoragePolicyDescriptorPtr & desc)
{
    Poco::AutoPtr<Poco::XML::Document> xml_document(new Poco::XML::Document());
    Poco::AutoPtr<Poco::XML::Element> root(xml_document->createElement("policy"));
    xml_document->appendChild(root);
    Poco::AutoPtr<Poco::XML::Element> policy_elem(xml_document->createElement(desc->name));
    root->appendChild(policy_elem);

    auto append_property = [&](Poco::AutoPtr<Poco::XML::Element> & parent, const std::string & name, const std::string value) {
        Poco::AutoPtr<Poco::XML::Element> key_elem(xml_document->createElement(name));
        Poco::AutoPtr<Poco::XML::Text> value_elem(xml_document->createTextNode(value));
        key_elem->appendChild(value_elem);
        parent->appendChild(key_elem);
    };

    Poco::AutoPtr<Poco::XML::Element> volumes_element(xml_document->createElement("volumes"));
    for (const auto & vol : desc->volumes)
    {
        Poco::AutoPtr<Poco::XML::Element> volume_element(xml_document->createElement(vol.name));

        /// max_data_part_size_bytes or max_data_part_size_ratio
        if (vol.max_data_part_size != 0)
            append_property(volume_element, "max_data_part_size_bytes", fmt::format("{}", vol.max_data_part_size));

        if (vol.max_data_part_size_ratio > 0.0)
            append_property(volume_element, "max_data_part_size_ratio", fmt::format("{}", vol.max_data_part_size_ratio));

        /// perform_ttl_move_on_insert, by default is true
        if (!vol.perform_ttl_move_on_insert)
            append_property(volume_element, "perform_ttl_move_on_insert", fmt::format("{}", vol.perform_ttl_move_on_insert));

        /// prefer_not_to_merge, by default is false
        if (vol.prefer_not_to_merge)
            append_property(volume_element, "prefer_not_to_merge", fmt::format("{}", vol.prefer_not_to_merge));

        /// load_balancing
        if (vol.load_balancing != cluster::protocol::VolumeDescriptor::VolumeLoadBalancing::Unknown)
        {
            auto val = std::string(magic_enum::enum_name(vol.load_balancing));
            std::transform(val.begin(), val.end(), val.begin(), ::tolower);
            append_property(volume_element, "load_balancing", val);
        }

        /// volume_priority — UINT64_MAX (default) and 0 (legacy pre-v2 metastore default) both
        /// mean "no explicit priority". A real explicit priority is 1..N.
        if (vol.volume_priority != std::numeric_limits<UInt64>::max() && vol.volume_priority != 0)
            append_property(volume_element, "volume_priority", fmt::format("{}", vol.volume_priority));

        /// least_used_ttl_ms — only emit when the user explicitly chose something other than the
        /// 60000 default, to keep the resulting XML compact for the common case.
        if (vol.least_used_ttl_ms != 60'000)
            append_property(volume_element, "least_used_ttl_ms", fmt::format("{}", vol.least_used_ttl_ms));

        /// create 'disks' elements
        auto append_disk = [&](const auto & name) {
            Poco::AutoPtr<Poco::XML::Element> disk_element(xml_document->createElement("disk"));
            Poco::AutoPtr<Poco::XML::Text> textNode = xml_document->createTextNode(name);
            disk_element->appendChild(textNode);
            volume_element->appendChild(disk_element);
        };
        std::for_each(vol.disk_names.begin(), vol.disk_names.end(), append_disk);

        volumes_element->appendChild(volume_element);
    }
    policy_elem->appendChild(volumes_element);

    /// move_factor
    append_property(policy_elem, "move_factor", fmt::format("{}", desc->move_factor));

    Poco::AutoPtr<Poco::Util::XMLConfiguration> conf(new Poco::Util::XMLConfiguration());
    conf->load(xml_document);
    return conf;
}

void createStoragePolicyFromDescriptor(const cluster::protocol::StoragePolicyDescriptorPtr & desc, ContextPtr context)
{
    context->getStoragePolicyFromConfiguration(desc->name, getStoragePolicyConfigurationFromDescriptor(desc));
}

}
