SELECT CounterID, count(), max_if(SearchPhrase, not_empty(SearchPhrase)) from table(test.hits) GROUP BY CounterID ORDER BY count() DESC LIMIT 20;
SELECT CounterID, count(), max_if(SearchPhrase, not_empty(SearchPhrase)) from table(test.hits) GROUP BY CounterID ORDER BY count() DESC LIMIT 20 SETTINGS optimize_aggregation_in_order = 1
