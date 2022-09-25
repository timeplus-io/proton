DROP STREAM IF EXISTS arraytest;

create stream arraytest ( created_date date DEFAULT to_date(created_at), created_at datetime DEFAULT now(), strings array(string) DEFAULT empty_array_string()) ENGINE = MergeTree(created_date, cityHash64(created_at), (created_date, cityHash64(created_at)), 8192);

INSERT INTO arraytest (created_at, strings) VALUES (now(), ['aaaaa', 'bbbbb', 'ccccc']);
INSERT INTO arraytest (created_at, strings) VALUES (now(), ['aaaaa', 'bbbbb', null]); -- { clientError 349 }

SELECT strings from arraytest;

DROP STREAM IF EXISTS arraytest;

