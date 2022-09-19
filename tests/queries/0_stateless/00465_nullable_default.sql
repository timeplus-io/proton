DROP STREAM IF EXISTS nullable_00465;
create stream nullable_00465 (id Nullable(uint32), cat string)  ;
INSERT INTO nullable_00465 (cat) VALUES ('test');
SELECT * FROM nullable_00465;
DROP STREAM nullable_00465;
