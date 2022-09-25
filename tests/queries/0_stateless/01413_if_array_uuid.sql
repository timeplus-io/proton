SELECT if(number % 2 = 0, [to_uuid('00000000-e1fe-11e9-bb8f-853d60c00749')], [to_uuid('11111111-e1fe-11e9-bb8f-853d60c00749')]) FROM numbers(5);
