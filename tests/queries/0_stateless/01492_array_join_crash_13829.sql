SELECT NULL = count_equal(materialize([array_join([NULL, NULL, NULL]), NULL AS x, array_join([255, 1025, NULL, NULL]), array_join([2, 1048576, NULL, NULL])]), materialize(x)) format Null;
