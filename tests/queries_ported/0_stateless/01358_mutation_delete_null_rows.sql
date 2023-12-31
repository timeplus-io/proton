select '--------';
SELECT array_join([0, 1, 3, NULL]) AS x,  x = 0,  if(x = 0, 'x=0', 'x<>0') ORDER BY x;

select '--------';
drop stream if exists mutation_delete_null_rows;

CREATE STREAM mutation_delete_null_rows
(
    `EventDate` Date,
    `CounterID` nullable(string),
    `UserID` nullable(uint32)
)
ENGINE = MergeTree()
ORDER BY EventDate;

INSERT INTO mutation_delete_null_rows VALUES ('2020-01-01', '', 2)('2020-01-02', 'aaa', 0);
INSERT INTO mutation_delete_null_rows VALUES ('2020-01-03', '', 2)('2020-01-04', '', 2)('2020-01-05', NULL, 2)('2020-01-06', 'aaa', 0)('2020-01-07', 'aaa', 0)('2020-01-08', 'aaa', NULL);

SELECT *,UserID = 0 as UserIDEquals0, if(UserID = 0, 'delete', 'leave') as verdict FROM mutation_delete_null_rows ORDER BY EventDate;

ALTER STREAM mutation_delete_null_rows DELETE WHERE UserID = 0 SETTINGS mutations_sync=1;

select '--------';
SELECT * FROM mutation_delete_null_rows ORDER BY EventDate;

drop stream mutation_delete_null_rows;
