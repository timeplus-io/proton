SELECT quantileTDigestWeighted(-0.)(to_datetime(10000000000.), 1); -- { serverError DECIMAL_OVERFLOW }
