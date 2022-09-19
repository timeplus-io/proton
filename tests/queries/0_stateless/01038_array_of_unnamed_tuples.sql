SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS array_of_tuples;

create stream array_of_tuples 
(
    f array(tuple(float64, float64)), 
    s array(tuple(uint8, uint16, uint32))
) ;

INSERT INTO array_of_tuples values ([(1, 2), (2, 3), (3, 4)], array(tuple(1, 2, 3), tuple(2, 3, 4))), (array((1.0, 2.0)), [tuple(4, 3, 1)]);

SELECT f from array_of_tuples;
SELECT s from array_of_tuples;

DROP STREAM array_of_tuples;
