SELECT finalize_aggregation(initializeAggregation('uniqExactState', toNullable('foo')));
SELECT to_type_name(initializeAggregation('uniqExactState', toNullable('foo')));

SELECT finalize_aggregation(initializeAggregation('uniqExactState', toNullable(123)));
SELECT to_type_name(initializeAggregation('uniqExactState', toNullable(123)));

SELECT initializeAggregation('uniqExactState', toNullable('foo')) = arrayReduce('uniqExactState', [toNullable('foo')]);
SELECT initializeAggregation('uniqExactState', toNullable(123)) = arrayReduce('uniqExactState', [toNullable(123)]);
