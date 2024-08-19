CREATE STREAM 99006_v(id int);

INSERT INTO 99006_v(id) values(3);

DROP STREAM 99006_v settings force_drop_big_stream = true;
