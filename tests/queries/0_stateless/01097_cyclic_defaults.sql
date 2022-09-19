DROP STREAM IF EXISTS table_with_cyclic_defaults;

create stream table_with_cyclic_defaults (a DEFAULT b, b DEFAULT a) ; --{serverError 174}

create stream table_with_cyclic_defaults (a DEFAULT b + 1, b DEFAULT a * a) ; --{serverError 174}

create stream table_with_cyclic_defaults (a DEFAULT b, b DEFAULT to_string(c), c DEFAULT concat(a, '1')) ; --{serverError 174}

create stream table_with_cyclic_defaults (a DEFAULT b, b DEFAULT c, c DEFAULT a * b) ; --{serverError 174}

create stream table_with_cyclic_defaults (a string DEFAULT b, b string DEFAULT a) ; --{serverError 174}

create stream table_with_cyclic_defaults (a string) ;

ALTER STREAM table_with_cyclic_defaults ADD COLUMN c string DEFAULT b, ADD COLUMN b string DEFAULT c; --{serverError 174}

ALTER STREAM table_with_cyclic_defaults ADD COLUMN b string DEFAULT a, MODIFY COLUMN a DEFAULT b; --{serverError 174}

SELECT 1;

DROP STREAM IF EXISTS table_with_cyclic_defaults;
