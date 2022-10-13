SELECT sum(length(ParsedParams.Key1)) from table(test.hits) WHERE not_empty(ParsedParams.Key1);
SELECT sum(length(ParsedParams.ValueDouble)) from table(test.hits) WHERE not_empty(ParsedParams.ValueDouble);
