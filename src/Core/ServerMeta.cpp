#include <Core/ServerMeta.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

void ServerMeta::load(const fs::path & server_meta_file, Poco::Logger * logger)
{
    /// Write a meta file containing a unique uuid and its epoch / boostrap time etc if the file doesn't already exist during server start.

    if (fs::exists(server_meta_file))
    {
        try
        {
            /// Read from meta file
            {
                ReadBufferFromFile in(server_meta_file);

                /// When we have update this metadata file in future,
                /// we will use the version for backward compatibility
                readIntText(version, in);

                assertString("\n", in);

                readUUIDText(server_uuid, in);

                assertString("\n", in);

                readIntText(epoch, in);

                /// assertEOF(in);
            }
        }
        catch (...)
        {
            LOG_PANIC(
                logger, "Cannot read server meta data from file {}: {}.", server_meta_file.string(), getCurrentExceptionMessage(true));
        }
    }

    /// Override or create
    try
    {
        if (server_uuid == UUIDHelpers::Nil)
            server_uuid = UUIDHelpers::generateV4();

        /// Every boot, increase the epoch once
        epoch += 1;

        fs::path tmp_meta_file = server_meta_file;
        tmp_meta_file.replace_extension(".tmp");

        {
            WriteBufferFromFile out(tmp_meta_file);

            auto out_str = toString(server_uuid);
            out_str = fmt::format("{}\n{}\n{}", version, out_str, epoch);

            out.write(out_str.data(), out_str.size());
            out.sync();
            out.finalize();
        }

        /// Rename to override / create
        fs::rename(tmp_meta_file, server_meta_file);

        boot_timestamp = DB::UTCMilliseconds::now();
    }
    catch (...)
    {
        LOG_PANIC(
            logger,
            "Caught Exception {} while writing the Server meta file {}",
            getCurrentExceptionMessage(false),
            server_meta_file.string());
    }
}

}
