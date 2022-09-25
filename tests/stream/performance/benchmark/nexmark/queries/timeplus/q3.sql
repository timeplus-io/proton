-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  name  string,
  city  string,
  state  string,
  id  int64
);

INSERT INTO discard_sink (name, city, state, id)
SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA') settings seek_to='earliest';



