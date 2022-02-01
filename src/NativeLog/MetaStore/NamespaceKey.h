#include <rocksdb/slice_transform.h>
#include <rocksdb/slice.h>

namespace nlog
{
/// NamespaceTransform checks if a Slice is in namespace `Domain`. If yes, it extracts
/// the namespace from the Slice and RocksDB will build bloomfilter for the extracted namespaces
/// It is assumed that every key  is in the form of
/// [NAMESPACE_SEPARATOR][internal-namespace][NAMESPACE_SEPARATOR][namespace][NAMESPACE_SEPARATOR][key]
/// For example, /topics/ns1/topic1
class NamespaceTransform : public rocksdb::SliceTransform
{
public:
    /// @param ns_sep_ Namespace separator. By default, it's `/`
    explicit NamespaceTransform(char ns_sep_ = '/') : ns_sep(ns_sep_) { }

    const char * Name() const override { return "NamespaceTransform"; }

    rocksdb::Slice Transform(const rocksdb::Slice & s) const override { return doTransform(s); }

    bool InDomain(const rocksdb::Slice & s) const override
    {
        auto res = doTransform(s);
        return !res.empty();
    }

private:
    rocksdb::Slice doTransform(const rocksdb::Slice & s) const;

private:
    char ns_sep;
};

/// Concatenate namespaces and key by using namespace separator
/// For example `/ns1/topic1`
std::string namespaceKey(const std::string & ns, const std::string & key = "", char ns_sep = '/');
}
