SET send_logs_level = 'fatal';
SET allow_suspicious_codecs = 1;

DROP STREAM IF EXISTS delta_codec_for_alter;
create stream delta_codec_for_alter (date date, x uint32 Codec(Delta), s FixedString(128)) ENGINE = MergeTree ORDER BY tuple();
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'delta_codec_for_alter' AND name = 'x';
ALTER STREAM delta_codec_for_alter MODIFY COLUMN x Codec(Delta, LZ4);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'delta_codec_for_alter' AND name = 'x';
ALTER STREAM delta_codec_for_alter MODIFY COLUMN x uint64 Codec(Delta, LZ4);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'delta_codec_for_alter' AND name = 'x';
DROP STREAM IF EXISTS delta_codec_for_alter;
