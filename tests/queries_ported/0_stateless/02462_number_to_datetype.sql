-- { echoOn }

-- to_date
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_int64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_uint64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_int32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_uint32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_float32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_float64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);

-- to_date
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_int64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_uint64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_int32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_uint32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_float32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_date(recordTimestamp, 'Europe/Amsterdam')), to_date(recordTimestamp, 'Europe/Amsterdam'), to_float64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);

-- to_datetime
select to_YYYYMMDD(to_datetime(recordTimestamp, 'Europe/Amsterdam')), to_datetime(recordTimestamp, 'Europe/Amsterdam'), to_int64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime(recordTimestamp, 'Europe/Amsterdam')), to_datetime(recordTimestamp, 'Europe/Amsterdam'), to_uint64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime(recordTimestamp, 'Europe/Amsterdam')), to_datetime(recordTimestamp, 'Europe/Amsterdam'), to_int32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime(recordTimestamp, 'Europe/Amsterdam')), to_datetime(recordTimestamp, 'Europe/Amsterdam'), to_uint32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime(recordTimestamp, 'Europe/Amsterdam')), to_datetime(recordTimestamp, 'Europe/Amsterdam'), to_float32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime(recordTimestamp, 'Europe/Amsterdam')), to_datetime(recordTimestamp, 'Europe/Amsterdam'), to_float64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);

-- to_datetime64
select to_YYYYMMDD(to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam')), to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam'), to_int64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam')), to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam'), to_uint64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam')), to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam'), to_int32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam')), to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam'), to_uint32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam')), to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam'), to_float32(1665519765) as recordTimestamp, to_type_name(recordTimestamp);
select to_YYYYMMDD(to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam')), to_datetime64(recordTimestamp, 3, 'Europe/Amsterdam'), to_float64(1665519765) as recordTimestamp, to_type_name(recordTimestamp);

-- { echoOff }
