SET query_mode='table';
-- Repro on MutableStream

DROP STREAM IF EXISTS 99073_jsn_num_str_mut;
CREATE STREAM 99073_jsn_num_str_mut (cl_ord_id string, v int) PRIMARY KEY cl_ord_id;

DROP STREAM IF EXISTS 99072_jsn_num_str;
CREATE STREAM 99072_jsn_num_str (cl_ord_id string);


-- Default is on: numeric value for a String column should succeed
INSERT INTO 99073_jsn_num_str_mut FORMAT JSONEachRow {"cl_ord_id": 12345, "v": 1};

INSERT INTO 99072_jsn_num_str FORMAT JSONEachRow {"cl_ord_id": 12345};

SELECT sleep(3) FORMAT Null;

SELECT cl_ord_id, v FROM 99073_jsn_num_str_mut ORDER BY cl_ord_id;
SELECT cl_ord_id FROM 99072_jsn_num_str ORDER BY cl_ord_id;

-- Disable and verify failure
SET input_format_json_read_numbers_as_strings = 0;
INSERT INTO 99073_jsn_num_str_mut FORMAT JSONEachRow {"cl_ord_id": 67890, "v": 2}; -- { clientError CANNOT_PARSE_QUOTED_STRING }

INSERT INTO 99072_jsn_num_str FORMAT JSONEachRow {"cl_ord_id": 67890}; -- { clientError CANNOT_PARSE_QUOTED_STRING }

DROP STREAM 99072_jsn_num_str;


DROP STREAM 99073_jsn_num_str_mut;
