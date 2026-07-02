CREATE DATABASE IF NOT EXISTS test_99050;

DROP STREAM IF EXISTS test_99050.99050_hcs;
DROP STORAGE POLICY IF EXISTS 99050_p1;
DROP DISK IF EXISTS 99050_d1;

CREATE DISK IF NOT EXISTS 99050_d1 disk(type = local, path = '/var/lib/proton/disks/d1/');
CREATE DISK IF NOT EXISTS 99050_d2 disk(type = invalid, path = '/var/lib/proton/disks/d1/'); -- { serverError BAD_ARGUMENTS }

-- Test SHOW CREATE DISK functionality
SHOW CREATE DISK 99050_d1;
SHOW CREATE DISK nonexistent_disk; -- { serverError UNKNOWN_DISK }

create STORAGE POLICY IF NOT EXISTS 99050_p1 as $$
volumes:
    hot:
        disk: default
    cold:
        disk: 99050_d1
$$;

create STORAGE POLICY IF NOT EXISTS 99050_p2 as $$
    hot:
        disk: default
$$; -- { serverError NO_ELEMENTS_IN_CONFIG }

create STORAGE POLICY IF NOT EXISTS 99050_p2 as $$
volumes:
    hot:
        disk: default
    cold:
        disk: 99050_d2
$$; -- { serverError INVALID_CONFIG_PARAMETER }

create STORAGE POLICY IF NOT EXISTS 99050_p3 as $$
volumes:
    hot:
    cold:
$$; -- { serverError NO_ELEMENTS_IN_CONFIG }

-- Test SHOW CREATE DISK with different disk types
CREATE DISK IF NOT EXISTS 99050_d3 disk(type = local, path = '/var/lib/proton/disks/d3/', max_data_part_size_bytes = 1048576);
SHOW CREATE DISK 99050_d3;

CREATE STREAM test_99050.99050_hcs
(
    `i32` int32, `s` string, `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, LZ4), INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 2
)
PARTITION BY to_YYYYMMDD(_tp_time)
ORDER BY to_start_of_hour(_tp_time)
TTL to_start_of_day(_tp_time) + interval 7 day to volume 'cold'
SETTINGS storage_policy = '99050_p1';

insert into test_99050.99050_hcs (i32, s, _tp_time) values (3, 'iphone', '2022-01-01 12:00:00') (12, 'android', now());

SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;

SELECT table, rows, disk_name, active FROM system.parts WHERE table = '99050_hcs';

SELECT i32, s from table(test_99050.99050_hcs) order by i32;

-- Test SHOW CREATE DISK for default disk
SHOW CREATE DISK default;

DROP STREAM test_99050.99050_hcs;
DROP STORAGE POLICY 99050_p1;

DROP DISK 99050_d1;
DROP DISK 99050_d3;
DROP DISK 99050_d2; -- { serverError UNKNOWN_DISK }

-- Test SHOW CREATE DISK after dropping disk
SHOW CREATE DISK 99050_d3; -- { serverError UNKNOWN_DISK }
