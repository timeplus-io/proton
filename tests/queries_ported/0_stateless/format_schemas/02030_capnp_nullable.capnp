@0x9ef128e10a8010b2;

struct NullableUInt64
{
    union
    {
        value @0 : uint64;
        null @1 : Void;
    }
}

struct tuple
{
    nullable @0 : NullableUInt64;
}

struct Message
{
    nullable @0 : NullableUInt64;
    array @1 : List(NullableUInt64);
    tuple @2 : tuple;
}
