#include <Checkpoint/Checkpoint.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/RocksCheckpoint.h>

#include <Cluster/Common/serde.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
}

const std::unordered_map<CheckpointType, std::string_view> & Checkpoint::supportedTypesAndPostfixes()
{
    static const std::unordered_map<CheckpointType, std::string_view> types_and_postfixes{
        {CheckpointType::File, FileCheckpoint::CKPT_POSTFIX},
        {CheckpointType::Rocks, RocksCheckpoint::CKPT_POSTFIX},
    };
    return types_and_postfixes;
}

std::string_view Checkpoint::postfix(CheckpointType type)
{
    const auto & types_and_postfixes = supportedTypesAndPostfixes();
    auto it = types_and_postfixes.find(type);
    if (it == types_and_postfixes.end())
        throw Exception(ErrorCodes::UNSUPPORTED, "Unsupported checkpoint type: {}", magic_enum::enum_name(type));

    return it->second;
}

CheckpointPtr Checkpoint::createFrom(CheckpointType type, const DiskPath & ckpt_path)
{
    chassert(postfix(type) == ckpt_path.path.extension().string());

    CheckpointPtr ckpt;
    switch (type)
    {
        case CheckpointType::File:
        {
            ckpt = std::make_shared<FileCheckpoint>(ckpt_path);
            break;
        }
        case CheckpointType::Rocks:
        {
            ckpt = std::make_shared<RocksCheckpoint>(ckpt_path);
            break;
        }
        default:
            throw Exception(ErrorCodes::UNSUPPORTED, "Unsupported checkpoint type: {}", magic_enum::enum_name(type));
    }
    return ckpt;
}

void Checkpoint::serialize(WriteBuffer & wb, bool compress_entity)
{
    /// Layout: [version][type][entity(ckpt data)]
    writeVarUInt(version, wb);
    cluster::serializeEnum<CheckpointType>(type(), wb);
    serializeEntity(wb, compress_entity);
}

CheckpointPtr Checkpoint::deserialize(std::unique_ptr<ReadBuffer> ckpt_buf, bool entity_compressed)
{
    VersionType version_;
    readVarUInt(version_, *ckpt_buf);

    auto type_ = cluster::deserializeEnum<CheckpointType>(*ckpt_buf);
    CheckpointPtr ckpt;
    switch (type_)
    {
        case CheckpointType::File:
        {
            ckpt = std::make_shared<FileCheckpoint>(version_, std::move(ckpt_buf), entity_compressed);
            break;
        }
        case CheckpointType::Rocks:
        {
            ckpt = std::make_shared<RocksCheckpoint>(version_, std::move(ckpt_buf), entity_compressed);
            break;
        }
        default:
            throw Exception(ErrorCodes::UNSUPPORTED, "Unsupported checkpoint type: {}", magic_enum::enum_name(type_));
    }
    return ckpt;
}
}
