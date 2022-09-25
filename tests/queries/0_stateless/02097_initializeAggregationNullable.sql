SELECT finalize_aggregation(initializeAggregation('uniqExactState', to_nullable('foo')));
SELECT to_type_name(initializeAggregation('uniqExactState', to_nullable('foo')));

SELECT finalize_aggregation(initializeAggregation('uniqExactState', to_nullable(123)));
SELECT to_type_name(initializeAggregation('uniqExactState', to_nullable(123)));

SELECT initializeAggregation('uniqExactState', to_nullable('foo')) = array_reduce('uniqExactState', [to_nullable('foo')]);
SELECT initializeAggregation('uniqExactState', to_nullable(123)) = array_reduce('uniqExactState', [to_nullable(123)]);
