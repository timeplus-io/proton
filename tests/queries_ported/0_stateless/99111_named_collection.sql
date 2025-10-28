DROP DISK IF EXISTS 99111_d1;
DROP DISK IF EXISTS 99111_d3;
DROP NAMED COLLECTION IF EXISTS nc1;
DROP NAMED COLLECTION IF EXISTS nc2;

CREATE NAMED COLLECTION
    nc1
AS
    type = 'local',
    path = '/var/lib/timeplusd/disks/99111d1/';

CREATE DISK IF NOT EXISTS 99111_d1 disk(named_collection=nc1);
SELECT name, path, type FROM system.disks WHERE name = '99111_d1';

-- Override with invalid type
CREATE DISK IF NOT EXISTS 99111_d2 disk(named_collection=nc1, type=invalid); -- { serverError BAD_ARGUMENTS }

-- Override with path
CREATE DISK IF NOT EXISTS 99111_d3 disk(named_collection=nc1, path = '/var/lib/timeplusd/disks/99111d3/');
SELECT name, path, type FROM system.disks WHERE name = '99111_d3';

CREATE NAMED COLLECTION
    nc2
AS
    type = 'local' NOT OVERRIDABLE,
    path = '/var/lib/timeplusd/disks/99111d4/' NOT OVERRIDABLE;

-- Not overridable setting
CREATE DISK IF NOT EXISTS 99111_d4 disk(named_collection=nc2, type=local); -- { serverError BAD_ARGUMENTS }

DROP DISK IF EXISTS 99111_d1;
DROP DISK IF EXISTS 99111_d3;
DROP NAMED COLLECTION IF EXISTS nc1;
DROP NAMED COLLECTION IF EXISTS nc2;
