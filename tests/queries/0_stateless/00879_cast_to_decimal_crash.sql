select cast(toIntervalDay(1) as nullable(Decimal(10, 10))); -- { serverError 70 }
