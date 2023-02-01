CREATE STREAM test
(
    `a0` uint64 DEFAULT a1 + 1,
    `a1` uint64 DEFAULT a0 + 1,
    `a2` uint64 DEFAULT a3 + a4,
    `a3` uint64 DEFAULT a2 + 1,
    `a4` uint64 ALIAS a3 + 1
)
ENGINE = Log; -- { serverError 174 }

CREATE STREAM pythagoras
(
    `a` float64 DEFAULT sqrt((c * c) - (b * b)),
    `b` float64 DEFAULT sqrt((c * c) - (a * a)),
    `c` float64 DEFAULT sqrt((a * a) + (b * b))
)
ENGINE = Log; -- { serverError 174 }

-- TODO: It works but should not: CREATE STREAM test (a DEFAULT b, b DEFAULT a) ENGINE = Memory
