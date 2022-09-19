DROP STREAM IF EXISTS defaults;
create stream defaults (a uint8, b DEFAULT 0, c DEFAULT identity(b)) ;
INSERT INTO defaults (a) VALUES (1);
SELECT * FROM defaults;
DROP STREAM defaults;

DROP STREAM IF EXISTS elog_cut;
create stream elog_cut
(
    date date DEFAULT to_date(uts),
    uts DateTime,
    pr uint64,
    ya_uid uint64,
    adf_uid uint64,
    owner_id uint32,
    eff_uid uint64 DEFAULT if(adf_uid != 0, adf_uid, ya_uid),
    page_session uint64 DEFAULT cityHash64(eff_uid, pr),
    sample_key uint64 ALIAS page_session
) ENGINE = MergeTree(date, cityHash64(adf_uid, ya_uid, pr), (owner_id, date, cityHash64(adf_uid, ya_uid, pr)), 8192);

INSERT INTO elog_cut (uts, pr, ya_uid, adf_uid, owner_id) VALUES ('2015-01-01 01:02:03', 111, 123, 456, 789);
SELECT date, uts, pr, ya_uid, adf_uid, owner_id, eff_uid, page_session, sample_key FROM elog_cut;
DROP STREAM elog_cut;
