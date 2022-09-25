-- { echo }
SELECT toUInt32OrDefault(to_nullable(to_uint32(1))) SETTINGS cast_keep_nullable=1;
SELECT toUInt32OrDefault(to_nullable(to_uint32(1)), to_nullable(to_uint32(2))) SETTINGS cast_keep_nullable=1;
SELECT toUInt32OrDefault(to_uint32(1)) SETTINGS cast_keep_nullable=1;
SELECT toUInt32OrDefault(to_uint32(1), to_uint32(2)) SETTINGS cast_keep_nullable=1;
