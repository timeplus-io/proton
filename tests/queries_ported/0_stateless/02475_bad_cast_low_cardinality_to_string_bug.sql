SELECT if(materialize(0), extract(materialize(CAST('aaaaaa', 'low_cardinality(string)')), '\\w'), extract(materialize(CAST('bbbbb', 'low_cardinality(string)')), '\\w*')) AS res FROM numbers(2);
