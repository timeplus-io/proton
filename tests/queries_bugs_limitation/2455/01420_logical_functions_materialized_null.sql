SELECT NULL AND 1;
SELECT NULL OR 1;
SELECT materialize(NULL) AND 1;
SELECT materialize(NULL) OR 1;
SELECT array_join([NULL]) AND 1;
SELECT array_join([NULL]) OR 1;

SELECT is_constant(array_join([NULL]) AND 1);
