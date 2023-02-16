#!/usr/bin/env bash
# Tags: no-fasttest

# End-to-end test of serialization/deserialization of a stream with different
# data types to/from ProtobufList format.
#   Cf. 00825_protobuf_format_persons.sh

# To generate reference file for this test use the following commands:
# ninja ProtobufDelimitedMessagesSerializer
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP STREAM IF EXISTS persons_02240;
DROP STREAM IF EXISTS roundtrip_persons_02240;
DROP STREAM IF EXISTS alt_persons_02240;
DROP STREAM IF EXISTS str_persons_02240;
DROP STREAM IF EXISTS syntax2_persons_02240;

CREATE STREAM persons_02240 (uuid uuid,
                            name string,
                            surname string,
                            gender enum8('male'=1, 'female'=0),
                            birthDate Date,
                            photo nullable(string),
                            phoneNumber nullable(fixed_string(13)),
                            isOnline uint8,
                            visitTime nullable(DateTime('Europe/Moscow')),
                            age uint8,
                            zodiacSign enum16('aries'=321, 'taurus'=420, 'gemini'=521, 'cancer'=621, 'leo'=723, 'virgo'=823,
                                              'libra'=923, 'scorpius'=1023, 'sagittarius'=1122, 'capricorn'=1222, 'aquarius'=120,
                                              'pisces'=219),
                            songs array(string),
                            color array(uint8),
                            hometown low_cardinality(string),
                            location array(decimal32(6)),
                            pi nullable(float64),
                            lotteryWin nullable(decimal64(2)),
                            someRatio float32,
                            temperature decimal32(1),
                            randomBigNumber int64,
                            measureUnits Nested(unit  string, coef float32),
                            nestiness_a_b_c_d nullable(uint32),
                            "nestiness_a_B.c_E" array(uint32)
                           ) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO persons_02240 VALUES (to_uuid('a7522158-3d41-4b77-ad69-6c598ee55c49'), 'Ivan', 'Petrov', 'male', to_date('1980-12-29'), 'png', '+74951234567', 1, to_datetime('2019-01-05 18:45:00', 'Europe/Moscow'), 38, 'capricorn', ['Yesterday', 'Flowers'], [255, 0, 0], 'Moscow', [55.753215, 37.622504], 3.14, 214.10, 0.1, 5.8, 17060000000, ['meter', 'centimeter', 'kilometer'], [1, 0.01, 1000], 500, [501, 502]);
INSERT INTO persons_02240 VALUES (to_uuid('c694ad8a-f714-4ea3-907d-fd54fb25d9b5'), 'Natalia', 'Sokolova', 'female', to_date('1992-03-08'), 'jpg', NULL, 0, NULL, 26, 'pisces', [], [100, 200, 50], 'Plymouth', [50.403724, -4.142123], 3.14159, NULL, 0.007, 5.4, -20000000000000, [], [], NULL, []);
INSERT INTO persons_02240 VALUES (to_uuid('a7da1aa6-f425-4789-8947-b034786ed374'), 'Vasily', 'Sidorov', 'male', to_date('1995-07-28'), 'bmp', '+442012345678', 1, to_datetime('2018-12-30 00:00:00', 'Europe/Moscow'), 23, 'leo', ['Sunny'], [250, 244, 10], 'Murmansk', [68.970682, 33.074981], 3.14159265358979, 100000000000, 800, -3.2, 154400000, ['pound'], [16], 503, []);

SELECT * FROM persons_02240 ORDER BY name;
EOF

# Note: if you actually look into below used schemafiles, you find that the message payload was duplicated. This is a workaround caused by Google protoc
# not being able to decode or reference nested elements, only top-level elements. In theory, we could make protoc read the top-level Envelope message but even
# that is not possible if it is length-delimited (and it is). Protobuf_length_delimited_encoder.py with '--format "protobuflist"' takes care to remove
# the top level Envelope message manually so that the remaining (binary) nested message looks to protoc like instances of the duplicated messages. Not pretty
# but does the job ...

# Use schema 02240_protobuflist1_format_persons:Person
echo
echo "Schema 02240_protobuflist1_format_persons:Person"
BINARY_FILE_PATH=$(mktemp "$CURDIR/02240_protobuflist1_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_02240 ORDER BY name FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/02240_protobuflist1_format_persons:Person'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/02240_protobuflist1_format_persons:Person" --input "$BINARY_FILE_PATH" --format "protobuflist"
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE STREAM roundtrip_persons_02240 AS persons_02240"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip_persons_02240 SETTINGS format_schema='$SCHEMADIR/02240_protobuflist1_format_persons:Person' FORMAT ProtobufList" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_persons_02240 ORDER BY name"
rm "$BINARY_FILE_PATH"

# Use schema 02240_protobuflist2_format_persons:AltPerson
echo
echo "Schema 02240_protobuflist2_format_persons:AltPerson"
BINARY_FILE_PATH=$(mktemp "$CURDIR/02240_protobuflist2_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_02240 ORDER BY name FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/02240_protobuflist2_format_persons:AltPerson'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/02240_protobuflist2_format_persons:AltPerson" --input "$BINARY_FILE_PATH" --format="protobuflist"
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE STREAM alt_persons_02240 AS persons_02240"
$CLICKHOUSE_CLIENT --query "INSERT INTO alt_persons_02240 SETTINGS format_schema='$SCHEMADIR/02240_protobuflist2_format_persons:AltPerson' FORMAT ProtobufList" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM alt_persons_02240 ORDER BY name"
rm "$BINARY_FILE_PATH"

# Use schema 02240_protobuflist3_format_persons:StrPerson
echo
echo "Schema 02240_protobuflist3_format_persons:StrPerson as ProtobufList"
BINARY_FILE_PATH=$(mktemp "$CURDIR/02240_protobuflist3_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_02240 ORDER BY name FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/02240_protobuflist3_format_persons:StrPerson'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/02240_protobuflist3_format_persons:StrPerson" --input "$BINARY_FILE_PATH" --format="protobuflist"
# echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE STREAM str_persons_02240 AS persons_02240"
$CLICKHOUSE_CLIENT --query "INSERT INTO str_persons_02240 SETTINGS format_schema='$SCHEMADIR/02240_protobuflist3_format_persons:StrPerson' FORMAT ProtobufList" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM str_persons_02240 ORDER BY name"
rm "$BINARY_FILE_PATH"

# Use schema 02240_protobuflist_format_syntax2:Syntax2Person
echo
echo "Schema 02240_protobuf_format_syntax2:Syntax2Person"
BINARY_FILE_PATH=$(mktemp "$CURDIR/02240_protobuflist_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_02240 ORDER BY name FORMAT ProtobufList SETTINGS format_schema = '$SCHEMADIR/02240_protobuflist_format_persons_syntax2:Syntax2Person'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/02240_protobuflist_format_persons_syntax2:Syntax2Person" --input "$BINARY_FILE_PATH" --format="protobuflist"
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE STREAM syntax2_persons_02240 AS persons_02240"
$CLICKHOUSE_CLIENT --query "INSERT INTO syntax2_persons_02240 SETTINGS format_schema='$SCHEMADIR/02240_protobuflist_format_persons_syntax2:Syntax2Person' FORMAT ProtobufList" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM syntax2_persons_02240 ORDER BY name"
rm "$BINARY_FILE_PATH"

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP STREAM persons_02240;
DROP STREAM roundtrip_persons_02240;
DROP STREAM alt_persons_02240;
DROP STREAM str_persons_02240;
DROP STREAM syntax2_persons_02240;
EOF
