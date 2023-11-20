#include <Common/FieldVisitorSum.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


FieldVisitorSum::FieldVisitorSum(const Field & rhs_) : rhs(rhs_) {}

// We can add all ints as unsigned regardless of their actual signedness.
bool FieldVisitorSum::operator() (Int64 & x) const { return this->operator()(reinterpret_cast<UInt64 &>(x)); }
bool FieldVisitorSum::operator() (UInt64 & x) const
{
    x += applyVisitor(FieldVisitorConvertToNumber<UInt64>(), rhs);
    return x != 0;
}

bool FieldVisitorSum::operator() (Float64 & x) const { x += rhs.get<Float64>(); return x != 0; }

bool FieldVisitorSum::operator() (Null &) const
{
    /// Do not add anything
    return false;
}

bool FieldVisitorSum::operator() (String &) const { throw Exception("Cannot sum strings", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Array &) const { throw Exception("Cannot sum arrays", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Tuple &) const { throw Exception("Cannot sum tuples", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Map &) const { throw Exception("Cannot sum maps", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (Object &) const { throw Exception("Cannot sum objects", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (UUID &) const { throw Exception("Cannot sum uuids", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (IPv4 &) const { throw Exception("Cannot sum ipv4s", ErrorCodes::LOGICAL_ERROR); }
bool FieldVisitorSum::operator() (IPv6 &) const { throw Exception("Cannot sum ipv6s", ErrorCodes::LOGICAL_ERROR); }


bool FieldVisitorSum::operator() (AggregateFunctionStateData &) const
{
    throw Exception("Cannot sum AggregateFunctionStates", ErrorCodes::LOGICAL_ERROR);
}

bool FieldVisitorSum::operator() (bool &) const { throw Exception("Cannot sum bools", ErrorCodes::LOGICAL_ERROR); }

}

