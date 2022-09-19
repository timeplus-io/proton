SELECT sum(length(ParsedParams.Key1)) FROM test.hits WHERE not_empty(ParsedParams.Key1);
SELECT sum(length(ParsedParams.ValueDouble)) FROM test.hits WHERE not_empty(ParsedParams.ValueDouble);
