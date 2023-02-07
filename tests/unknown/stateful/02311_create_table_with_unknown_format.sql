-- Tags: no-fasttest, use-hdfs

create stream test_02311 (x uint32) engine=File(UnknownFormat); -- {serverError UNKNOWN_FORMAT}
create stream test_02311 (x uint32) engine=URL('http://some/url', UnknownFormat); -- {serverError UNKNOWN_FORMAT}
create stream test_02311 (x uint32) engine=S3('http://host:2020/test/data', UnknownFormat); -- {serverError UNKNOWN_FORMAT}
create stream test_02311 (x uint32) engine=HDFS('http://hdfs:9000/data', UnknownFormat); -- {serverError UNKNOWN_FORMAT}
