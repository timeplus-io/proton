-- length should be 32
select length(hex(generateUUIDv4()));

with generateUUIDv4() as uuid,
    replace(to_string(uuid), '-', '') as str1,
    lower(hex(uuid)) as str2
select str1 = str2;

-- hex on uuid always generate 32 characters even there're leading zeros
select lower(hex(to_uuid('00000000-80e7-46f8-0000-9d773a2fd319')));

-- length should be 128
select length(bin(generateUUIDv4()));

-- bin on uuid always generate 128 characters even there're leading zeros
select bin(to_uuid('00000000-80e7-46f8-0000-9d773a2fd319'));