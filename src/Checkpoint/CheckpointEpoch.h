#pragma once

#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include <fmt/format.h>

#include <compare>
#include <stdexcept>

namespace DB
{
struct CheckpointEpoch
{
    int64_t epoch = 0;
    std::string ulid = {};

    void reset()
    {
        epoch = 0;
        ulid.clear();
    }

    bool empty() const { return epoch <= 0; }

    auto operator<=>(const CheckpointEpoch &) const = default;

    std::string toString() const { return ulid.empty() ? fmt::format("{}", epoch) : fmt::format("{}_{}", epoch, ulid); }

    void serialize(WriteBuffer & wb) const
    {
        DB::writeVarInt(epoch, wb);
        DB::writeStringBinary(ulid, wb);
    }

    void deserialize(ReadBuffer & rb)
    {
        DB::readVarInt(epoch, rb);
        DB::readStringBinary(ulid, rb);
    }

    static CheckpointEpoch parse(const std::string & s)
    {
        CheckpointEpoch ckpt_epoch;
        size_t pos = 0;
        ckpt_epoch.epoch = std::stoll(s, &pos);
        /// There are two cases: "12_{ulid}" and "12" (no ulid, for backward compatibility)
        if (pos + 1 < s.size())
            ckpt_epoch.ulid = s.substr(pos + 1);

        return ckpt_epoch;
    }
};
}

namespace fmt
{
template <>
struct formatter<DB::CheckpointEpoch>
{
    constexpr static auto parse(format_parse_context & ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const DB::CheckpointEpoch & v, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", v.toString());
    }
};
}
