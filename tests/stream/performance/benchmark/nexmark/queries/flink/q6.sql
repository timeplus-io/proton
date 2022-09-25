-- -------------------------------------------------------------------------------------------------
-- Query 6: Average Selling Price by Seller
-- -------------------------------------------------------------------------------------------------
-- What is the average selling price per seller for their last 10 closed auctions.
-- Shares the same ‘winning bids’ core as for Query4, and illustrates a specialized combiner.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  seller VARCHAR,
  avg_price  BIGINT
) WITH (
  'connector' = 'blackhole'
);

-- TODO: this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't
--  support to consume retractions.
INSERT INTO discard_sink
SELECT
    Q.seller,
    AVG(Q.final) OVER
        (PARTITION BY Q.seller ORDER BY Q.dateTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM (
    SELECT MAX(B.price) AS final, A.seller, B.dateTime
    FROM auction AS A, bid AS B
    WHERE A.id = B.auction and B.dateTime between A.dateTime and A.expires
    GROUP BY A.id, A.seller
) AS Q;