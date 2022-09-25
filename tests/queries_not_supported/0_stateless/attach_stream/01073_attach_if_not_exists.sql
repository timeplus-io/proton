-- Tags: no-parallel

create stream aine (a int)  ;
ATTACH STREAM aine; -- { serverError 57 }
ATTACH STREAM IF NOT EXISTS aine;
DETACH STREAM aine;
ATTACH STREAM IF NOT EXISTS aine;
EXISTS TABLE aine;
DROP STREAM aine;
