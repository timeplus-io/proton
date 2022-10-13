SELECT count() from table(test.hits) WHERE NOT (EventDate >= to_date('2015-01-01') AND EventDate < to_date('2015-02-01'))
