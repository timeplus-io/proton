SELECT JSONExtractKeysAndValuesRaw(array_join([])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT JSONHas(array_join([]));  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT isValidJSON(array_join([]));  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT concat(array_join([]), array_join([NULL, ''])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT plus(array_join([]), array_join([NULL, 1])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT sipHash64(array_join([]), [NULL], array_join(['', NULL, '', NULL])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT [concat(NULL, array_join([]))];
