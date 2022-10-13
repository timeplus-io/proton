SELECT ParsedParams.Key1 FROM table(test.visits) FINAL WHERE VisitID != 0 AND not_empty(ParsedParams.Key1) ORDER BY VisitID LIMIT 10

