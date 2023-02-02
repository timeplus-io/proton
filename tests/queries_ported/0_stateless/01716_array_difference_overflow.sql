-- Overflow is Ok and behaves as the CPU does it.
SELECT array_difference([65536, -9223372036854775808]);

-- Diff of unsigned int -> int
SELECT array_difference( cast([10, 1], 'array(uint8)'));
SELECT array_difference( cast([10, 1], 'array(uint16)'));
SELECT array_difference( cast([10, 1], 'array(uint32)'));
SELECT array_difference( cast([10, 1], 'array(uint64)'));
