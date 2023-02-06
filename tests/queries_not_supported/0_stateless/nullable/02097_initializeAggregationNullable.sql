SELECT finalizeAggregation(initializeAggregation('uniqExactState', to_nullable('foo')));
SELECT to_type_name(initializeAggregation('uniqExactState', to_nullable('foo')));

SELECT finalizeAggregation(initializeAggregation('uniqExactState', to_nullable(123)));
SELECT to_type_name(initializeAggregation('uniqExactState', to_nullable(123)));

SELECT initializeAggregation('uniqExactState', to_nullable('foo')) = arrayReduce('uniqExactState', [to_nullable('foo')]);
SELECT initializeAggregation('uniqExactState', to_nullable(123)) = arrayReduce('uniqExactState', [to_nullable(123)]);
