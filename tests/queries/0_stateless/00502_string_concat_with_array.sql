select  a, b || b from (select [number] as a, to_string(number) as b from system.numbers limit 2);
