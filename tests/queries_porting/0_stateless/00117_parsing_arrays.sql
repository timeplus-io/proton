SET query_mode='table';
DROP STREAM IF EXISTS null_00117;
create stream null_00117 (a array(uint64), b array(string), c array(array(date))) ;

INSERT INTO null_00117 (a) VALUES ([1,2]), ([3, 4]), ([ 5 ,6]), ([	7  ,   8  	  ]), ([]), ([   ]);
INSERT INTO null_00117 (b) VALUES ([ 'Hello' , 'World' ]);
INSERT INTO null_00117 (c) VALUES ([	]), ([ [ ] ]), ([[],[]]), ([['2015-01-01', '2015-01-02'], ['2015-01-03', '2015-01-04']]);

SELECT a, b, c FROM null_00117 ORDER BY a, b, c;

DROP STREAM null_00117;