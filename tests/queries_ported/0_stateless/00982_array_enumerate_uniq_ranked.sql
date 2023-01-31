SELECT array_enumerate_uniq_ranked(x, 2) FROM VALUES('x array(array(string))', ([[]]), ([['a'], ['a'], ['b']]), ([['a'], ['a'], ['b']]));
