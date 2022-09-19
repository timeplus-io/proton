SET allow_experimental_map_type = 1;
SET output_format_write_statistics = 0;

DROP STREAM IF EXISTS map_formats;
create stream map_formats (m Map(string, uint32), m1 Map(string, date), m2 Map(string, array(uint32)))  ;

INSERT INTO map_formats VALUES(map('k1', 1, 'k2', 2, 'k3', 3), map('k1', to_date('2020-05-05')), map('k1', [], 'k2', [7, 8]));
INSERT INTO map_formats VALUES(map('k1', 10, 'k3', 30), map('k2', to_date('2020-06-06')), map());

SELECT 'JSON';
SELECT * FROM map_formats ORDER BY m['k1'] FORMAT JSON;
SELECT 'JSONEachRow';
SELECT * FROM map_formats ORDER BY m['k1'] FORMAT JSONEachRow;
SELECT 'CSV';
SELECT * FROM map_formats ORDER BY m['k1'] FORMAT CSV;
SELECT 'TSV';
SELECT * FROM map_formats ORDER BY m['k1'] FORMAT TSV;
SELECT 'TSKV';
SELECT * FROM map_formats ORDER BY m['k1'] FORMAT TSKV;

DROP STREAM map_formats;
