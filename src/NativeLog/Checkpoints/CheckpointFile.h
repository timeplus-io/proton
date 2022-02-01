#pragma once

#include <NativeLog/Base/Stds.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>

#include <mutex>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int BAD_VERSION;
    extern const int FILE_CORRUPTION;
}
}

namespace nlog
{
/// This class represents a utility to capture a checkpoint in a file. It writes down to the file in the below format.
/// ------file beginning------
/// version: int32_t
/// entries-count: int32_t
/// entry-as-string-on-each-line
/// ------file end------
/// CheckpointFile is multi-thread safe
template <typename T>
class CheckpointFile final
{
public:
    CheckpointFile(const fs::path & file_, int32_t version_) : file(file_), temp_file(file_.string() + ".tmp"), version(version_) { }

    void write(const std::vector<T> & entries)
    {
        std::lock_guard<std::mutex> guard{lock};

        /// Write to temp file first
        DB::WriteBufferFromFile wbuf{temp_file};

        /// Write version
        DB::writeText(version, wbuf);
        DB::writeText('\n', wbuf);

        /// Write entries count
        DB::writeText(entries.size(), wbuf);
        DB::writeText('\n', wbuf);

        /// Write each entry on a new line
        for (const auto & entry : entries)
        {
            entry.write(wbuf);
            DB::writeText('\n', wbuf);
        }
        wbuf.sync();

        /// Rename temp file to target file
        std::filesystem::rename(temp_file, file);
    }

    std::vector<T> read()
    {
        std::lock_guard<std::mutex> guard{lock};

        DB::ReadBufferFromFile rbuf{file};

        std::string data;
        DB::readText(data, rbuf);
        auto rversion = DB::parseIntStrict<int32_t>(data, 0, data.size());
        if (rversion != version)
            throw DB::Exception(
                "Unrecognized checkpoint version: " + data + " , expected version: " + std::to_string(version),
                DB::ErrorCodes::BAD_VERSION);

        DB::assertString("\n", rbuf);

        data.clear();
        DB::readText(data, rbuf);
        auto size = DB::parseIntStrict<int32_t>(data, 0, data.size());
        assert(size >= 0);

        std::vector<T> results;
        results.reserve(size);

        while (size > 0)
        {
            data.clear();
            DB::readText(data, rbuf);
            if (data.empty())
                throw DB::Exception(
                    fmt::format(
                        "Checkpoint corruption. Expected [{}] entries in checkpoint file but only got [{}] entries",
                        std::to_string(results.capacity()),
                        std::to_string(results.size())),
                    DB::ErrorCodes::FILE_CORRUPTION);

            results.push_back(T::read(data));
            DB::assertString("\n", rbuf);
            --size;
        }

        return results;
    }

private:
    std::mutex lock;
    std::string file;
    std::string temp_file;
    int32_t version;
};
}
