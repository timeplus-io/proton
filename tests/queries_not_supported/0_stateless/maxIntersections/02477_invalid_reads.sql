-- MIN, MAX AND FAMILY should check for errors in its input
SELECT finalize_aggregation(CAST(unhex('0F00000030'), 'aggregate_function(min, string)')); -- { serverError 33 }
SELECT finalize_aggregation(CAST(unhex('FFFF000030'), 'aggregate_function(min, string)')); -- { serverError 33 }

-- UBSAN
SELECT 'ubsan', hex(finalize_aggregation(CAST(unhex('4000000030313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233010000000000000000'),
                                             'aggregate_function(arg_max, string, uint64)')));

-- aggThrow should check for errors in its input
SELECT finalize_aggregation(CAST('', 'aggregate_function(aggThrow(0.), uint8)')); -- { serverError 32 }

-- categoricalInformationValue should check for errors in its input
SELECT finalize_aggregation(CAST(unhex('01000000000000000100000000000000'),
                                'aggregate_function(categoricalInformationValue, uint8, uint8)')); -- { serverError 33 }
SELECT finalize_aggregation(CAST(unhex('0101000000000000000100000000000000020000000000000001000000000000'),
    'aggregate_function(categoricalInformationValue, Nullable(uint8), uint8)')); -- { serverError 33 }

-- groupArray should check for errors in its input
SELECT finalize_aggregation(CAST(unhex('5FF3001310132'), 'aggregate_function(groupArray, string)'));  -- { serverError 33 }
SELECT finalize_aggregation(CAST(unhex('FF000000000000000001000000000000000200000000000000'), 'aggregate_function(groupArray, uint64)')); -- { serverError 33 }

-- Same for groupArrayMovingXXXX
SELECT finalize_aggregation(CAST(unhex('0FF00000000000000001000000000000000300000000000000'), 'aggregate_function(groupArrayMovingSum, uint64)')); -- { serverError 33 }
SELECT finalize_aggregation(CAST(unhex('0FF00000000000000001000000000000000300000000000000'), 'aggregate_function(groupArrayMovingAvg, uint64)')); -- { serverError 33 }

-- Histogram
SELECT finalize_aggregation(CAST(unhex('00000000000024C000000000000018C00500000000000024C0000000000000F03F00000000000022C0000000000000F03F00000000000020C0000000000000'),
    'aggregate_function(histogram(5), int64)')); -- { serverError 33 }

-- StatisticalSample
SELECT finalize_aggregation(CAST(unhex('0F01000000000000244000000000000026400000000000002840000000000000244000000000000026400000000000002840000000000000F03F'),
                                'aggregate_function(mannWhitneyUTest, Float64, uint8)')); -- { serverError 33 }

-- maxIntersections
SELECT finalize_aggregation(CAST(unhex('0F010000000000000001000000000000000300000000000000FFFFFFFFFFFFFFFF03340B9B047F000001000000000000000500000065000000FFFFFFFFFFFFFFFF'),
                                'aggregate_function(maxIntersections, uint8, uint8)')); -- { serverError 33 }

-- sequenceNextNode (This was fine because it would fail in the next readBinary call, but better to add a test)
SELECT finalize_aggregation(CAST(unhex('FFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
                                'aggregate_function(sequenceNextNode(''forward'', ''head''), DateTime, Nullable(string), uint8, Nullable(uint8))'))
    SETTINGS allow_experimental_funnel_functions=1; -- { serverError 33 }

-- Fuzzer (ALL)
SELECT finalize_aggregation(CAST(unhex('FFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
                                'aggregate_function(sequenceNextNode(\'forward\', \'head\'), DateTime, Nullable(string), uint8, Nullable(uint8))'))
    SETTINGS allow_experimental_funnel_functions = 1; -- { serverError 128 }

-- Fuzzer 2 (UBSAN)
SELECT finalize_aggregation(CAST(unhex('FFFFFFF014181056F38010000000000000001FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
                                'aggregate_function(sequenceNextNode(\'forward\', \'head\'), DateTime, Nullable(string), uint8, Nullable(uint8))'))
    SETTINGS allow_experimental_funnel_functions = 1; -- { serverError 33 }

-- uniqUpTo
SELECT finalize_aggregation(CAST(unhex('04128345AA2BC97190'),
                                'aggregate_function(uniqUpTo(10), string)')); -- { serverError 33 }

-- quantiles
SELECT finalize_aggregation(CAST(unhex('0F0000000000000000'),
                                'aggregate_function(quantileExact, uint64)')); -- { serverError 33 }
SELECT finalize_aggregation(CAST(unhex('0F000000000000803F'),
                                'aggregate_function(quantileTDigest, uint64)')); -- { serverError 33 }
