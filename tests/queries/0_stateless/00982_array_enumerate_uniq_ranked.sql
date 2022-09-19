SELECT arrayEnumerateUniqRanked(x, 2) FROM VALUES('x array(array(string))', ([[]]), ([['a'], ['a'], ['b']]), ([['a'], ['a'], ['b']]));
