SELECT multiMatchAny('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multiMatchAnyIndex('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multi_search_all_positions('hello, world', ['hello', 'world']);
