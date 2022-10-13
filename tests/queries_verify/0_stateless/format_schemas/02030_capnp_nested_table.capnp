@0x9ef128e10a8010b3;


struct Nested1
{
    one @0 : uint64;
    two @1 : uint64;
}

struct nested
{
    value @0 : List(uint64);
    array @1 : List(List(uint64));
    tuple @2 : List(Nested1);
}

struct Message
{
    nested @0 : nested;
}
