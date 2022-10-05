-- -------------------------------------------------------------------------------------------------
-- Query 6: Average Selling Price by Seller
-- -------------------------------------------------------------------------------------------------
-- What is the average selling price per seller for their last 10 closed auctions.
-- Shares the same ‘winning bids’ core as for Query4, and illustrates a specialized combiner.
-- -------------------------------------------------------------------------------------------------
-- seller VARCHAR, but chanage to int64 looks more reasonable due to seller is all number


CREATE TABLE discard_sink (
  seller int64,
  avg_price  int64
);

-- TODO: this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't
--  support to consume retractions.
-- Timeplus: 
INSERT INTO discard_sink
SELECT
    Q.seller,
    array_avg([Q.final] || lags(Q.final, 1, 9, 0)) OVER
        (PARTITION BY Q.seller)
FROM (
    SELECT max(B.price) AS final, A.seller, B.dateTime
    FROM auction as A join bid as B
    on A.id = B.auction where B.dateTime between A.dateTime and A.expires
    GROUP BY A.id, A.seller, B.dateTime settings seek_to='earliest'
) AS Q;