SET send_logs_level = 'fatal';
SELECT array_enumerate_uniq(anyHeavy([]), []);
SELECT arrayEnumerateDense([], [sequenceCount(NULL)]); -- { serverError 190 }
SELECT arrayEnumerateDense([STDDEV_SAMP(NULL, 910947.571364)], [NULL]);
