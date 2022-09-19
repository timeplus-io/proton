SELECT 0b0001 as number, to_type_name(number);
SELECT 0b0010 as number, to_type_name(number);
SELECT 0b0100 as number, to_type_name(number);
SELECT 0b1000 as number, to_type_name(number);

SELECT 'Unsigned numbers';
SELECT 0b10000000 as number, to_type_name(number);
SELECT 0b1000000000000000 as number, to_type_name(number);
SELECT 0b10000000000000000000000000000000 as number, to_type_name(number);
SELECT 0b1000000000000000000000000000000000000000000000000000000000000000 as number, to_type_name(number);

SELECT 'Signed numbers';
SELECT -0b10000000 as number, to_type_name(number);
SELECT -0b1000000000000000 as number, to_type_name(number);
SELECT -0b10000000000000000000000000000000 as number, to_type_name(number);
SELECT -0b1000000000000000000000000000000000000000000000000000000000000000 as number, to_type_name(number);
