SELECT multi_match_any('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multi_match_any_index('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multi_search_all_positions('hello, world', ['hello', 'world']);
