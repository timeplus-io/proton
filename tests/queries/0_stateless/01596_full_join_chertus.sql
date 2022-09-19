select to_type_name(materialize(js1.k)), to_type_name(materialize(js2.k)), to_type_name(materialize(js1.s)), to_type_name(materialize(js2.s))
from (select number k, toLowCardinality(to_string(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, to_string(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;

select to_type_name(js1.k), to_type_name(js2.k), to_type_name(js1.s), to_type_name(js2.s)
from (select number k, toLowCardinality(to_string(number)) s from numbers(2)) as js1
full join (select toLowCardinality(number+1) k, to_string(number+1) s from numbers(2)) as js2
using k order by js1.k, js2.k;
