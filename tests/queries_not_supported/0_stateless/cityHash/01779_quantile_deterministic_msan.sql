SELECT cityHash64(to_string(quantileDeterministicState(number, sipHash64(number)))) FROM numbers(8193);
