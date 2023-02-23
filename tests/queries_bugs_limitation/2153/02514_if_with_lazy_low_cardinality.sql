create stream if not exists t (`arr.key` array(low_cardinality(string)), `arr.value` array(low_cardinality(string))) engine = Memory;
insert into t (`arr.key`, `arr.value`) values (['a'], ['b']);
select if(true, if(lowerUTF8(arr.key) = 'a', 1, 2), 3) as x from t left array join arr;
drop stream t;

