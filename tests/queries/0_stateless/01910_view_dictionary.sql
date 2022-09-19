-- Tags: no-parallel

DROP STREAM IF EXISTS dictionary_source_en;
DROP STREAM IF EXISTS dictionary_source_ru;
DROP STREAM IF EXISTS dictionary_source_view;
DROP DICTIONARY IF EXISTS flat_dictionary;

create stream dictionary_source_en
(
    id uint64,
    value string
) ;

INSERT INTO dictionary_source_en VALUES (1, 'One'), (2,'Two'), (3, 'Three');

create stream dictionary_source_ru
(
    id uint64,
    value string
) ;

INSERT INTO dictionary_source_ru VALUES (1, 'Один'), (2,'Два'), (3, 'Три');

CREATE VIEW dictionary_source_view AS  SELECT id, dictionary_source_en.value as value_en, dictionary_source_ru.value as value_ru  FROM  dictionary_source_en LEFT JOIN dictionary_source_ru USING (id);

select * from dictionary_source_view;

CREATE DICTIONARY flat_dictionary
(
    id uint64,
    value_en string,
    value_ru string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' PASSWORD '' TABLE 'dictionary_source_view'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(FLAT());

SELECT
    dictGet(concat(currentDatabase(), '.flat_dictionary'), 'value_en', number + 1),
    dictGet(concat(currentDatabase(), '.flat_dictionary'), 'value_ru', number + 1)
FROM numbers(3);

DROP STREAM dictionary_source_en;
DROP STREAM dictionary_source_ru;
DROP STREAM dictionary_source_view;
DROP DICTIONARY flat_dictionary;
