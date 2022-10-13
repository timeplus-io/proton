SELECT count() from table(test.hits) PREWHERE UserID IN (SELECT UserID from table(test.hits) WHERE CounterID = 800784);
