#pragma once

#include <Cluster/Common/FetchHint.h>
#include <Cluster/LocalLog/Common/LogSequenceMetadata.h>
#include <Cluster/Common/Entry.h>

namespace cluster::nlog
{
struct FetchResult
{
    bool empty() const { return entries.empty(); }

    int64_t log_start_sn = 0;
    int64_t log_committed_sn = 0;

    /// `fetch_hint` tells if it is not null option
    ///    a) The next sn to fetch
    ///    b) The file position for this next sn
    FetchHint fetch_hint;

    /// `fetched_log_sn_metadata` tells
    ///    a) The first sn fetched
    ///    b) The file position for the first sn fetched
    LogSequenceMetadata fetched_log_sn_metadata = {};

    cluster::EntryPtrs entries = {};
};
}
