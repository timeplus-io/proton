-- https://github.com/ClickHouse/ClickHouse/issues/43247
SELECT finalize_aggregation(CAST('aggregate_function(categoricalInformationValue, nullable(uint8), uint8)aggregate_function(categoricalInformationValue, nullable(uint8), uint8)',
                           'aggregate_function(min, string)')); -- { serverError CANNOT_READ_ALL_DATA }

-- Value from hex(minState('0123456789012345678901234567890123456789012345678901234567890123')). Size 63 + 1 (64)
SELECT finalize_aggregation(CAST(unhex('4000000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233'),
                           'aggregate_function(min, string)'));
