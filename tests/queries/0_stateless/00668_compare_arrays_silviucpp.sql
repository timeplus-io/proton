DROP STREAM IF EXISTS array;
create stream array (arr array(nullable(float64))) ;
INSERT INTO array(arr) values ([1,2]),([3,4]),([5,6]),([7,8]);

select * from array where arr > [12.2];
select * from array where arr > [null, 12.2];
select * from array where arr > [null, 12];

DROP STREAM array;
