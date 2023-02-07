SELECT if(number % 2, NULL, to_nullable(1)) FROM numbers(2);
SELECT if(number % 2, to_nullable(1), NULL) FROM numbers(2);

