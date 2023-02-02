-- Tags: no-fasttest

SELECT number FROM numbers(11) ORDER BY array_join(['а', 'я', '\0�', '', 'Я', '']) ASC, to_string(number) ASC, 'y' ASC COLLATE 'el';
