-- Tags: no-fasttest

SELECT generate_ulid(1) != generate_ulid(2), to_type_name(generate_ulid());
