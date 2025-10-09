-- Cleanup existing streams
DROP STREAM IF EXISTS `temp_c[1]_joined`;

CREATE STREAM `temp_c[1]_joined`
(
  `f01` datetime64(3, 'UTC'),
  `f02` datetime64(3, 'UTC'),
  `f03` int64,
  `f04` string,
  `f05` string,
  `f06` string,
  `f07` string,
  `f08` string,
  `f09` string,
  `f10` string,
  `_tp_time` datetime64(3, 'UTC'),
  `f11` nullable(decimal(38, 4)),
  `f12` nullable(decimal(38, 4)),
  `f13` nullable(decimal(38, 4)),
  `f14` nullable(decimal(38, 4)),
  `f15` nullable(decimal(38, 4)),
  `f16` nullable(decimal(38, 4)),
  `f17` nullable(decimal(38, 4)),
  `f18` nullable(decimal(38, 4)),
  `f19` nullable(decimal(38, 4)),
  `f20` nullable(decimal(38, 4)),
  `f21` nullable(decimal(38, 4)),
  `f22` nullable(decimal(38, 4)),
  `f23` nullable(decimal(38, 4)),
  `f24` nullable(decimal(38, 4)),
  `f25` nullable(decimal(38, 4)),
  `f26` nullable(decimal(38, 4)),
  `f27` nullable(decimal(38, 4)),
  `f28` nullable(decimal(38, 4)),
  `f29` nullable(decimal(38, 4)),
  `f30` nullable(decimal(38, 4)),
  `f31` nullable(decimal(38, 4)),
  `f32` nullable(decimal(38, 4)),
  `f33` nullable(decimal(38, 4)),
  `f34` nullable(decimal(38, 4)),
  `f35` nullable(decimal(38, 4)),
  `f36` nullable(decimal(38, 4)),
  `f37` nullable(decimal(38, 4)),
  `f38` nullable(decimal(38, 4)),
  `f39` nullable(decimal(38, 4)),
  `f40` decimal(38, 4),
  `f41` nullable(decimal(38, 4)),
  `f42` nullable(decimal(38, 4)),
  `f43` nullable(decimal(38, 4)),
  `f44` nullable(decimal(38, 4)),
  `f45` nullable(decimal(20, 10)),
  `f46` nullable(decimal(20, 10)),
  `f47` nullable(decimal(20, 10)),
  `f48` nullable(decimal(20, 10)),
  `f49` nullable(decimal(20, 10)),
  `f50` nullable(decimal(20, 10)),
  `f51` nullable(decimal(16, 4)),
  `f52` nullable(decimal(16, 4)),
  `f53` nullable(date),
  `f54` nullable(string),
  `f55` nullable(decimal(18, 2)),
  `f56` nullable(int32),
  `f57` nullable(int32),
  `f58` nullable(decimal(38, 2)),
  `f59` nullable(decimal(38, 2)),
  `f60` nullable(decimal(38, 10)),
  `f61` nullable(decimal(38, 2)),
  `f62` nullable(decimal(38, 10)),
  `f63` nullable(decimal(38, 10)),
  `f64` nullable(decimal(38, 10)),
  `f65` nullable(decimal(38, 4)),
  `f66` nullable(decimal(38, 4)),
  `f67` nullable(decimal(38, 4))
);

-- Shall have no ast size too big issue https://github.com/timeplus-io/proton-enterprise/issues/9167
SELECT
    multi_if(
            (`temp_c[1]_joined`.f11 IS NULL)
                AND (`temp_c[1]_joined`.f12 IS NULL)
                AND (`temp_c[1]_joined`.f13 IS NULL)
                AND (`temp_c[1]_joined`.f14 IS NULL)
                AND (`temp_c[1]_joined`.f16 IS NULL)
                AND (`temp_c[1]_joined`.f17 IS NULL)
                AND (`temp_c[1]_joined`.f15 IS NULL),
            NULL,
            (((((COALESCE(`temp_c[1]_joined`.f11, 0) + COALESCE(`temp_c[1]_joined`.f12, 0)) + COALESCE(`temp_c[1]_joined`.f13, 0)) + COALESCE(`temp_c[1]_joined`.f14, 0)) + COALESCE(`temp_c[1]_joined`.f16, 0)) + COALESCE(`temp_c[1]_joined`.f17, 0)) + COALESCE(`temp_c[1]_joined`.f15, 0)) AS f68,

    multi_if((`temp_c[1]_joined`.f18 IS NULL)
                 AND (`temp_c[1]_joined`.f19 IS NULL)
                 AND (`temp_c[1]_joined`.f20 IS NULL)
                 AND (`temp_c[1]_joined`.f21 IS NULL)
                 AND (`temp_c[1]_joined`.f22 IS NULL)
                 AND (`temp_c[1]_joined`.f23 IS NULL)
                 AND (`temp_c[1]_joined`.f25 IS NULL)
                 AND (`temp_c[1]_joined`.f26 IS NULL)
                 AND (`temp_c[1]_joined`.f27 IS NULL)
                 AND (`temp_c[1]_joined`.f28 IS NULL)
                 AND (`temp_c[1]_joined`.f29 IS NULL)
                 AND (`temp_c[1]_joined`.f30 IS NULL)
                 AND (`temp_c[1]_joined`.f31 IS NULL)
                 AND (`temp_c[1]_joined`.f32 IS NULL)
                 AND (`temp_c[1]_joined`.f33 IS NULL),
             NULL,
             (((((((((((((COALESCE(`temp_c[1]_joined`.f18, 0) + COALESCE(`temp_c[1]_joined`.f19, 0)) + COALESCE(`temp_c[1]_joined`.f20, 0)) + COALESCE(`temp_c[1]_joined`.f21, 0)) + COALESCE(`temp_c[1]_joined`.f22, 0)) + COALESCE(`temp_c[1]_joined`.f23, 0)) + COALESCE(`temp_c[1]_joined`.f25, 0)) + COALESCE(`temp_c[1]_joined`.f26, 0)) + COALESCE(`temp_c[1]_joined`.f27, 0)) + COALESCE(`temp_c[1]_joined`.f28, 0)) + COALESCE(`temp_c[1]_joined`.f29, 0)) + COALESCE(`temp_c[1]_joined`.f30, 0)) + COALESCE(`temp_c[1]_joined`.f31, 0)) + COALESCE(`temp_c[1]_joined`.f32, 0)) + COALESCE(`temp_c[1]_joined`.f33, 0)) AS f69,
    multi_if((f68 IS NULL) AND (f69 IS NULL),
             NULL,
             COALESCE(f68, 0) + COALESCE(f69, 0)) AS f70,
    multi_if((cast('0', 'int32') IS NULL) AND (COALESCE(`temp_c[1]_joined`.f25, 0) IS NULL),
             NULL,
             COALESCE(cast('0', 'int32'), 0) - COALESCE(COALESCE(`temp_c[1]_joined`.f25, 0), 0)) AS f71,
    multi_if((f68 IS NULL) AND (f69 IS NULL),
             NULL,
             COALESCE(f68, 0) + COALESCE(f69, 0)) AS f72,
    multi_if((cast('0', 'int32') IS NULL) AND (COALESCE(`temp_c[1]_joined`.f21, 0) IS NULL),
             NULL,
             COALESCE(cast('0', 'int32'), 0) - COALESCE(COALESCE(`temp_c[1]_joined`.f21, 0), 0)) AS f73,
    multi_if((COALESCE(`temp_c[1]_joined`.f27, 0) IS NULL) AND (COALESCE(`temp_c[1]_joined`.f22, 0) IS NULL),
             NULL,
             COALESCE(COALESCE(`temp_c[1]_joined`.f27, 0), 0) - COALESCE(COALESCE(`temp_c[1]_joined`.f22, 0), 0)) AS f74,
    multi_if((cast('0', 'int32') IS NULL) AND (COALESCE(f74, 0) IS NULL),
             NULL,
             COALESCE(cast('0', 'int32'), 0) - COALESCE(COALESCE(f74, 0), 0)) AS f75,
    multi_if((cast('0', 'int32') IS NULL) AND (COALESCE(`temp_c[1]_joined`.f30, 0) IS NULL),
             NULL,
             COALESCE(cast('0', 'int32'), 0) - COALESCE(COALESCE(`temp_c[1]_joined`.f30, 0), 0)) AS f76,
    multi_if((f72 IS NULL) AND (f71 IS NULL),
             NULL,
             COALESCE(f72, 0) + COALESCE(f71, 0)) AS f77,
    multi_if((f77 IS NULL) AND (cast('0', 'int32') IS NULL),
             NULL,
             least(COALESCE(f77, cast('0', 'int32')), COALESCE(cast('0', 'int32'), f77))) AS f78,
    multi_if((cast('0', 'int32') IS NULL) AND (COALESCE(`temp_c[1]_joined`.f18, 0) IS NULL),
             NULL,
             COALESCE(cast('0', 'int32'), 0) - COALESCE(COALESCE(`temp_c[1]_joined`.f18, 0), 0)) AS f79,
    multi_if((f78 IS NULL)
                 AND (f76 IS NULL)
                 AND (f75 IS NULL)
                 AND (f73 IS NULL),
             NULL,
             ((COALESCE(f78, 0) + COALESCE(f76, 0)) + COALESCE(f75, 0)) + COALESCE(f73, 0)) AS f80,
    multi_if((cast('0', 'int32') IS NULL) AND (f80 IS NULL),
             NULL,
             least(COALESCE(cast('0', 'int32'), f80), COALESCE(f80, cast('0', 'int32')))) AS f81,
    multi_if((cast('0', 'int32') IS NULL) AND (COALESCE(f81, 0) IS NULL),
             NULL,
             COALESCE(cast('0', 'int32'), 0) - COALESCE(COALESCE(f81, 0), 0)) AS f82,
    multi_if((f82 IS NULL) AND (f79 IS NULL),
             NULL,
             least(COALESCE(f82, f79), COALESCE(f79, f82))) AS f83,
    multi_if((f80 IS NULL) AND (f83 IS NULL),
             NULL,
             COALESCE(f80, 0) + COALESCE(f83, 0)) AS f84,
    round(f84, -2) AS f85
FROM  `temp_c[1]_joined`
SETTINGS query_mode = 'table';
