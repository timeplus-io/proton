SELECT round(array_join(categoricalInformationValue(x.1, x.2)), 3) FROM (SELECT array_join([(0, 0), (NULL, 2), (1, 0), (1, 1)]) AS x);
SELECT corr(c1, c2) FROM VALUES((0, 0), (NULL, 2), (1, 0), (1, 1));
SELECT round(array_join(categoricalInformationValue(c1, c2)), 3) FROM VALUES((0, 0), (NULL, 2), (1, 0), (1, 1));
SELECT round(array_join(categoricalInformationValue(c1, c2)), 3) FROM VALUES((0, 0), (NULL, 1), (1, 0), (1, 1));
SELECT categoricalInformationValue(c1, c2) FROM VALUES((0, 0), (NULL, 1));
SELECT categoricalInformationValue(c1, c2) FROM VALUES((NULL, 1)); -- { serverError 43 }
SELECT categoricalInformationValue(dummy, dummy);
SELECT categoricalInformationValue(dummy, dummy) WHERE 0;
SELECT categoricalInformationValue(c1, c2) FROM VALUES((to_nullable(0), 0));
SELECT groupUniqArray(*) FROM VALUES(to_nullable(0));
SELECT groupUniqArray(*) FROM VALUES(NULL);
SELECT categoricalInformationValue(c1, c2) FROM VALUES((NULL, NULL)); -- { serverError 43 }
SELECT categoricalInformationValue(c1, c2) FROM VALUES((0, 0), (NULL, 0));
SELECT quantiles(0.5, 0.9)(c1) FROM VALUES(0::nullable(uint8));
