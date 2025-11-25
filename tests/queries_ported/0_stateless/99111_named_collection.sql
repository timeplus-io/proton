DROP DISK IF EXISTS 99111_d1;
DROP DISK IF EXISTS 99111_d3;
DROP NAMED COLLECTION IF EXISTS nc1;
DROP NAMED COLLECTION IF EXISTS nc2;

CREATE NAMED COLLECTION
    nc1
AS
    type = 'local',
    path = '/var/lib/proton/disks/99111d1/';

CREATE DISK IF NOT EXISTS 99111_d1 disk(named_collection=nc1);
SELECT name, path, type FROM system.disks WHERE name = '99111_d1';

-- Override with invalid type
CREATE DISK IF NOT EXISTS 99111_d2 disk(named_collection=nc1, type=invalid); -- { serverError BAD_ARGUMENTS }

-- Override with path
CREATE DISK IF NOT EXISTS 99111_d3 disk(named_collection=nc1, path = '/var/lib/proton/disks/99111d3/');
SELECT name, path, type FROM system.disks WHERE name = '99111_d3';

CREATE NAMED COLLECTION
    nc2
AS
    type = 'local' NOT OVERRIDABLE,
    path = '/var/lib/proton/disks/99111d4/' NOT OVERRIDABLE;

-- Not overridable setting
CREATE DISK IF NOT EXISTS 99111_d4 disk(named_collection=nc2, type=local); -- { serverError BAD_ARGUMENTS }

-- Named collection already exists
CREATE NAMED COLLECTION nc2 AS foo='bar'; -- {serverError NAMED_COLLECTION_ALREADY_EXISTS }

CREATE NAMED COLLECTION IF NOT EXISTS nc2 AS type='invalid'; 
CREATE DISK IF NOT EXISTS 99111_d5 disk(named_collection=nc2);
SELECT name, path, type FROM system.disks WHERE name = '99111_d5';

DROP DISK IF EXISTS 99111_d1;
DROP DISK IF EXISTS 99111_d3;
DROP DISK IF EXISTS 99111_d5;
DROP NAMED COLLECTION IF EXISTS nc1;
DROP NAMED COLLECTION IF EXISTS nc2;
