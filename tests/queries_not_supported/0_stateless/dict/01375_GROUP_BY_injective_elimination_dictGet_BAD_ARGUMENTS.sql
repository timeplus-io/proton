SELECT dictGetString(concat('default', '.countryId'), 'country', to_uint64(number)) AS country FROM numbers(2) GROUP BY country; -- { serverError 36; }
