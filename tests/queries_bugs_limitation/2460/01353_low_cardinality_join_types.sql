set join_algorithm = 'hash';

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_string(number) as s from numbers(2)) as js1
join (select number+1 as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select number as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
join (select to_low_cardinality(number+1) as k, to_string(number+1) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
join (select to_low_cardinality(number+1) as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_string(number) as s from numbers(2)) as js1
full join (select number+1 as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select number as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
full join (select to_low_cardinality(number+1) as k, to_string(number+1) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
full join (select to_low_cardinality(number+1) as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

set join_algorithm = 'partial_merge';

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_string(number) as s from numbers(2)) as js1
join (select number+1 as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select number as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
join (select to_low_cardinality(number+1) as k, to_string(number+1) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
join (select to_low_cardinality(number+1) as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_string(number) as s from numbers(2)) as js1
full join (select number+1 as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select number as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
full join (select to_low_cardinality(number+1) as k, to_string(number+1) as s from numbers(2)) as js2
using k order by js1.k, js2.k;

select '-';
select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select to_low_cardinality(number) as k, to_low_cardinality(to_string(number)) as s from numbers(2)) as js1
full join (select to_low_cardinality(number+1) as k, to_low_cardinality(to_string(number+1)) as s from numbers(2)) as js2
using k order by js1.k, js2.k;
