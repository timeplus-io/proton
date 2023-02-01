DROP STREAM IF EXISTS table_with_cyclic_defaults;

CREATE STREAM table_with_cyclic_defaults (a DEFAULT b, b DEFAULT a) ENGINE = Memory; --{serverError 174}

CREATE STREAM table_with_cyclic_defaults (a DEFAULT b + 1, b DEFAULT a * a) ENGINE = Memory; --{serverError 174}

CREATE STREAM table_with_cyclic_defaults (a DEFAULT b, b DEFAULT toString(c), c DEFAULT concat(a, '1')) ENGINE = Memory; --{serverError 174}

CREATE STREAM table_with_cyclic_defaults (a DEFAULT b, b DEFAULT c, c DEFAULT a * b) ENGINE = Memory; --{serverError 174}

CREATE STREAM table_with_cyclic_defaults (a string DEFAULT b, b string DEFAULT a) ENGINE = Memory; --{serverError 174}

CREATE STREAM table_with_cyclic_defaults (a string) ENGINE = Memory;

ALTER STREAM table_with_cyclic_defaults ADD COLUMN c string DEFAULT b, ADD COLUMN b string DEFAULT c; --{serverError 174}

ALTER STREAM table_with_cyclic_defaults ADD COLUMN b string DEFAULT a, MODIFY COLUMN a DEFAULT b; --{serverError 174}

SELECT 1;

DROP STREAM IF EXISTS table_with_cyclic_defaults;
