
#include <rocksdb/comparator.h>

namespace nlog
{
template <typename Integer>
class IntegerComparator : public rocksdb::Comparator
{
public:
    IntegerComparator() { }

    const char * Name() const override { return "IntegerComparator"; }

    int Compare(const rocksdb::Slice & a, const rocksdb::Slice & b) const override
    {
        assert(a.size() == sizeof(Integer) && b.size() == sizeof(Integer));

        auto lhs = *reinterpret_cast<const Integer *>(a.data());
        auto rhs = *reinterpret_cast<const Integer *>(b.data());

        if (lhs == rhs)
            return 0;
        else if (lhs < rhs)
            return -1;
        else
            return 1;
    }

    void FindShortestSeparator(std::string *, const rocksdb::Slice &) const override { }

    void FindShortSuccessor(std::string *) const override { }
};

template <typename Integer>
const IntegerComparator<Integer> * integerComparator()
{
    static IntegerComparator<Integer> integer_comparator;
    return &integer_comparator;
}
}
