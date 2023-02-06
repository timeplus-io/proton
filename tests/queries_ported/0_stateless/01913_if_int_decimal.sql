select number % 2 ? materialize(1)::decimal(18, 10) : 2 FROM numbers(3);
