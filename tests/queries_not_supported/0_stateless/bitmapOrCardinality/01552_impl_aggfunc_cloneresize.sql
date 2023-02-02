drop stream if EXISTS test_bm;

drop stream if EXISTS test_bm_join;

create stream test_bm(
	dim uint64,
	id uint64 ) 
ENGINE = MergeTree()
ORDER BY( dim, id )
SETTINGS index_granularity = 8192;

create stream test_bm_join( 
  dim uint64,
  id uint64 )
ENGINE = MergeTree()
ORDER BY(dim,id)
SETTINGS index_granularity = 8192;

insert into test_bm VALUES (1,1),(2,2),(3,3),(4,4);

select
	dim ,
	sum(idnum)
from
	test_bm_join
right join(
	select
		dim,
		bitmapOrCardinality(ids,ids2) as idnum
	from
		(
		select
			dim,
			groupBitmapState(to_uint64(id)) as ids
		FROM
			test_bm
		where
			dim >2
		group by
			dim ) A all
	right join (
		select
			dim,
			groupBitmapState(to_uint64(id)) as ids2
		FROM
			test_bm
		where
			dim < 2
		group by
			dim ) B
	using(dim) ) C
using(dim)
group by dim;

drop stream test_bm;

drop stream test_bm_join;
