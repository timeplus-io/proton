SELECT if(materialize(1) > 0, CAST(NULL, 'nullable(int64)'), materialize(to_int32(1)));
SELECT if(materialize(1) > 0, materialize(to_int32(1)), CAST(NULL, 'nullable(int64)'));
SELECT if(materialize(1) > 0, CAST(NULL, 'nullable(decimal(18, 4))'), materialize(CAST(2, 'nullable(decimal(9, 4))')));
SELECT if(materialize(1) > 0, materialize(CAST(2, 'nullable(decimal(9, 4))')), CAST(NULL, 'nullable(decimal(18, 4))'));
