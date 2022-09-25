select (number % 2 <> 0) ? materialize(1)::Decimal(18, 10) : 2 FROM numbers(3);
