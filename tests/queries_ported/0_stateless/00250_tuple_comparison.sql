SELECT
    (1, 'Hello', 23) =  (1, 'Hello', 23),
    (1, 'Hello', 23) != (1, 'Hello', 23),
    (1, 'Hello', 23) <  (1, 'Hello', 23),
    (1, 'Hello', 23) >  (1, 'Hello', 23),
    (1, 'Hello', 23) <= (1, 'Hello', 23),
    (1, 'Hello', 23) >= (1, 'Hello', 23);
SELECT
    (1, 'Hello', 23) =  (2, 'Hello', 23),
    (1, 'Hello', 23) != (2, 'Hello', 23),
    (1, 'Hello', 23) <  (2, 'Hello', 23),
    (1, 'Hello', 23) >  (2, 'Hello', 23),
    (1, 'Hello', 23) <= (2, 'Hello', 23),
    (1, 'Hello', 23) >= (2, 'Hello', 23);
SELECT
    (1, 'Hello', 23) =  (1, 'World', 23),
    (1, 'Hello', 23) != (1, 'World', 23),
    (1, 'Hello', 23) <  (1, 'World', 23),
    (1, 'Hello', 23) >  (1, 'World', 23),
    (1, 'Hello', 23) <= (1, 'World', 23),
    (1, 'Hello', 23) >= (1, 'World', 23);
SELECT
    (1, 'Hello', 23) =  (1, 'Hello', 24),
    (1, 'Hello', 23) != (1, 'Hello', 24),
    (1, 'Hello', 23) <  (1, 'Hello', 24),
    (1, 'Hello', 23) >  (1, 'Hello', 24),
    (1, 'Hello', 23) <= (1, 'Hello', 24),
    (1, 'Hello', 23) >= (1, 'Hello', 24);
SELECT
    (2, 'Hello', 23) =  (1, 'Hello', 23),
    (2, 'Hello', 23) != (1, 'Hello', 23),
    (2, 'Hello', 23) <  (1, 'Hello', 23),
    (2, 'Hello', 23) >  (1, 'Hello', 23),
    (2, 'Hello', 23) <= (1, 'Hello', 23),
    (2, 'Hello', 23) >= (1, 'Hello', 23);
SELECT
    (1, 'World', 23) =  (1, 'Hello', 23),
    (1, 'World', 23) != (1, 'Hello', 23),
    (1, 'World', 23) <  (1, 'Hello', 23),
    (1, 'World', 23) >  (1, 'Hello', 23),
    (1, 'World', 23) <= (1, 'Hello', 23),
    (1, 'World', 23) >= (1, 'Hello', 23);
SELECT
    (1, 'Hello', 24) =  (1, 'Hello', 23),
    (1, 'Hello', 24) != (1, 'Hello', 23),
    (1, 'Hello', 24) <  (1, 'Hello', 23),
    (1, 'Hello', 24) >  (1, 'Hello', 23),
    (1, 'Hello', 24) <= (1, 'Hello', 23),
    (1, 'Hello', 24) >= (1, 'Hello', 23);
SELECT
    (1, 'Hello') =  (1, 'Hello'),
    (1, 'Hello') != (1, 'Hello'),
    (1, 'Hello') <  (1, 'Hello'),
    (1, 'Hello') >  (1, 'Hello'),
    (1, 'Hello') <= (1, 'Hello'),
    (1, 'Hello') >= (1, 'Hello');
SELECT
    (1, 'Hello') =  (2, 'Hello'),
    (1, 'Hello') != (2, 'Hello'),
    (1, 'Hello') <  (2, 'Hello'),
    (1, 'Hello') >  (2, 'Hello'),
    (1, 'Hello') <= (2, 'Hello'),
    (1, 'Hello') >= (2, 'Hello');
SELECT
    (1, 'Hello') =  (1, 'World'),
    (1, 'Hello') != (1, 'World'),
    (1, 'Hello') <  (1, 'World'),
    (1, 'Hello') >  (1, 'World'),
    (1, 'Hello') <= (1, 'World'),
    (1, 'Hello') >= (1, 'World');
SELECT
    (2, 'Hello') =  (1, 'Hello'),
    (2, 'Hello') != (1, 'Hello'),
    (2, 'Hello') <  (1, 'Hello'),
    (2, 'Hello') >  (1, 'Hello'),
    (2, 'Hello') <= (1, 'Hello'),
    (2, 'Hello') >= (1, 'Hello');
SELECT
    (1, 'World') =  (1, 'Hello'),
    (1, 'World') != (1, 'Hello'),
    (1, 'World') <  (1, 'Hello'),
    (1, 'World') >  (1, 'Hello'),
    (1, 'World') <= (1, 'Hello'),
    (1, 'World') >= (1, 'Hello');
SELECT
    (1) =  (1),
    (1) != (1),
    (1) <  (1),
    (1) >  (1),
    (1) <= (1),
    (1) >= (1);
SELECT
    (1) =  (2),
    (1) != (2),
    (1) <  (2),
    (1) >  (2),
    (1) <= (2),
    (1) >= (2);
SELECT
    (2) =  (1),
    (2) != (1),
    (2) <  (1),
    (2) >  (1),
    (2) <= (1),
    (2) >= (1);
SELECT
    (NULL) < (1),
    (NULL) = (1),
    (NULL) <= (1),
    (1, NULL) = (2, 1),
    (1, NULL) < (2, 1);
