#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace cluster::raft
{
template <typename K, typename V>
using HashMap = absl::flat_hash_map<K, V>;

template <typename K>
using HashSet = absl::flat_hash_set<K>;
}
