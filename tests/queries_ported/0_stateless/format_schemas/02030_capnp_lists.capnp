@0x9ef128e10a8010b7;

struct Message
{
    value @0 : uint64;
    list1 @1 : List(uint64);
    list2 @2 : List(List(List(uint64)));
}
