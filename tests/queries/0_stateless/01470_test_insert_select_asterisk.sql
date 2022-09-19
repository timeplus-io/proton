DROP STREAM IF EXISTS insert_select_dst;
DROP STREAM IF EXISTS insert_select_src;

create stream insert_select_dst (i int, middle_a int, middle_b int, j int)  ;

create stream insert_select_src (i int, j int)  ;

INSERT INTO insert_select_src VALUES (1, 2), (3, 4);

INSERT INTO insert_select_dst(* EXCEPT (middle_a, middle_b)) SELECT * FROM insert_select_src;
INSERT INTO insert_select_dst(insert_select_dst.* EXCEPT (middle_a, middle_b)) SELECT * FROM insert_select_src;
INSERT INTO insert_select_dst(COLUMNS('.*') EXCEPT (middle_a, middle_b)) SELECT * FROM insert_select_src;
INSERT INTO insert_select_dst(insert_select_src.* EXCEPT (middle_a, middle_b)) SELECT * FROM insert_select_src;  -- { serverError 47 }

SELECT * FROM insert_select_dst;

DROP STREAM IF EXISTS insert_select_dst;
DROP STREAM IF EXISTS insert_select_src;
