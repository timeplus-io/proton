-- Test ALTER EXTERNAL STREAM comment and settings

DROP STREAM IF EXISTS test_alter_external_stream;
DROP DATABASE IF EXISTS db99171 CASCADE;
CREATE DATABASE db99171;

CREATE STREAM db99171.hec_target1
(
  `_raw` string,
  `fields` map(string, string),
  `host` string,
  `_index` string,
  `source` string,
  `sourcetype` string
);

CREATE STREAM db99171.hec_target2
(
  `_raw` string,
  `fields` map(string, string),
  `host` string,
  `_index` string,
  `source` string,
  `sourcetype` string
);

CREATE EXTERNAL STREAM test_alter_external_stream
SETTINGS
    type = 'splunk-hec',
    tcp_port = 9171,
    target_stream = 'db99171.hec_target1';

-- Show create stream to verify initial comment and settings
SHOW CREATE test_alter_external_stream;

-- Test ALTER EXTERNAL STREAM SET COMMENT
ALTER STREAM test_alter_external_stream MODIFY COMMENT 'Comment v1';
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY COMMENT 'Comment v2';
SHOW CREATE test_alter_external_stream;

-- Test ALTER EXTERNAL STREAM MODIFY SETTING
ALTER STREAM test_alter_external_stream MODIFY SETTING type = 'splunk-hec-output'; -- { serverError UNSUPPORTED }
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY SETTING tcp_port = 9172;
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY SETTING target_stream = 'db99171.hec_target999'; -- { serverError UNKNOWN_STREAM }
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY SETTING target_stream = 'db99171.hec_target2';
SHOW CREATE test_alter_external_stream;

ALTER STREAM test_alter_external_stream MODIFY COMMENT 'Comment v3';
SHOW CREATE test_alter_external_stream;

-- Drop stream
DROP STREAM test_alter_external_stream;
DROP STREAM db99171.hec_target1;
DROP STREAM db99171.hec_target2;
DROP DATABASE db99171 CASCADE;
