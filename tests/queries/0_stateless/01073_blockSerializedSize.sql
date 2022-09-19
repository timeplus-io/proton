select 'uint8', blockSerializedSize(0);
select 'Nullable(uint8)', blockSerializedSize(toNullable(0));
select 'uint32', blockSerializedSize(0xdeadbeaf);
select 'uint64', blockSerializedSize(0xdeadbeafdead);
select 'Nullable(uint64)', blockSerializedSize(toNullable(0xdeadbeafdead));

select '';
select 'string', blockSerializedSize('foo');
select 'FixedString(32)', blockSerializedSize(cast('foo', 'FixedString(32)'));

select '';
select 'Enum8', blockSerializedSize(cast('a' as Enum8('a' = 1, 'b' = 2)));

select '';
select 'array', blockSerializedSize(['foo']);

select '';
select 'uniqCombinedState(100)', blockSerializedSize(uniqCombinedState(number)) from (select number from system.numbers limit 100);
select 'uniqCombinedState(10000)', blockSerializedSize(uniqCombinedState(number)) from (select number from system.numbers limit 10000);
select 'uniqCombinedState(100000)', blockSerializedSize(uniqCombinedState(number)) from (select number from system.numbers limit 100000);
select 'uniqCombinedState(1000000)', blockSerializedSize(uniqCombinedState(number)) from (select number from system.numbers limit 1000000);
select 'uniqCombinedState(10000000)', blockSerializedSize(uniqCombinedState(number)) from (select number from system.numbers limit 10000000);
select 'uniqCombined64State(10000000)', blockSerializedSize(uniqCombined64State(number)) from (select number from system.numbers limit 10000000);

select '';
select 'string,uint8', blockSerializedSize('foo', 1);

select '';
select 'Block(uint32)', blockSerializedSize(number) from numbers(2);
