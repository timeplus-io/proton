select cast(to_interval_day(1) as nullable(Decimal(10, 10))); -- { serverError 70 }
