#include <Dictionaries/HashedDictionary.h>

#include <Common/ArenaUtils.h>

namespace DB
{

template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ false, /* sharded= */ false >;
template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ false, /* sharded= */ true  >;

template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ false, /* sharded= */ false >;
template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ false, /* sharded= */ true  >;

}
