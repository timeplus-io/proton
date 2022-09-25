SET query_mode = 'table';
drop stream if exists radacct;
drop stream if exists mv_traffic_by_tadig15min;

create stream radacct ( radacctid uint64, f3gppchargingid Nullable(string), f3gppggsnaddress Nullable(string), f3gppggsnmccmnc Nullable(string), f3gppgprsqos Nullable(string), f3gppimeisv Nullable(string), f3gppimsi Nullable(uint64), f3gppimsimccmnc Nullable(string), f3gpploci Nullable(string), f3gppnsapi Nullable(string), f3gpprattype Nullable(string), f3gppsgsnaddress Nullable(string), f3gppsgsnmccmnc Nullable(string), acctdelaytime Nullable(uint32), acctinputoctets Nullable(uint64), acctinputpackets Nullable(uint64), acctoutputoctets Nullable(uint64), acctoutputpackets Nullable(uint64), acctsessionid string, acctstatustype Nullable(string), acctuniqueid string, calledstationid Nullable(string), callingstationid Nullable(string), framedipaddress Nullable(string), nasidentifier Nullable(string), nasipaddress Nullable(string), acctstarttime Nullable(DateTime), acctstoptime Nullable(DateTime), acctsessiontime Nullable(uint32), acctterminatecause Nullable(string), acctstartdelay Nullable(uint32), acctstopdelay Nullable(uint32), connectinfo_start Nullable(string), connectinfo_stop Nullable(string), timestamp DateTime, username Nullable(string), realm Nullable(string), f3gppimsi_int uint64, f3gppsgsnaddress_int Nullable(uint32), timestamp_date date, tac Nullable(string), mnc Nullable(string), tadig low_cardinality(string), country low_cardinality(string), tadig_op_ip Nullable(string) DEFAULT CAST('TADIG NOT FOUND', 'Nullable(string)'), mcc Nullable(uint16) MATERIALIZED toUInt16OrNull(substring(f3gppsgsnmccmnc, 1, 6))) ENGINE = MergeTree(timestamp_date, (timestamp, radacctid, acctuniqueid), 8192);

insert into radacct values (1, 'a', 'b', 'c', 'd', 'e', 2, 'a', 'b', 'c', 'd', 'e', 'f', 3, 4, 5, 6, 7, 'a', 'Stop', 'c', 'd', 'e', 'f', 'g', 'h', '2018-10-10 15:54:21', '2018-10-10 15:54:21', 8, 'a', 9, 10, 'a', 'b', '2018-10-10 15:54:21', 'a', 'b', 11, 12, '2018-10-10', 'a', 'b', 'c', 'd', 'e');

SELECT any(acctstatustype = 'Stop') FROM radacct WHERE (acctstatustype = 'Stop') AND ((acctinputoctets + acctoutputoctets) > 0);
create materialized view mv_traffic_by_tadig15min Engine=AggregatingMergeTree partition by tadig order by (ts,tadig) populate as select to_start_of_fifteen_minute(timestamp) ts,to_day_of_week(timestamp) dow, tadig, sumState(acctinputoctets+acctoutputoctets) traffic_bytes,maxState(timestamp) last_stop, minState(radacctid) min_radacctid,maxState(radacctid) max_radacctid from radacct where acctstatustype='Stop' and acctinputoctets+acctoutputoctets > 0 group by tadig,ts,dow;

select tadig, ts, dow, sumMerge(traffic_bytes), maxMerge(last_stop), minMerge(min_radacctid), maxMerge(max_radacctid) from mv_traffic_by_tadig15min group by tadig, ts, dow;

drop stream if exists radacct;
drop stream if exists mv_traffic_by_tadig15min;

