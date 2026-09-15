SELECT 
    text,
    CASE
        WHEN sentiment_score > 0 THEN 'positive'
        WHEN sentiment_score < 0 THEN 'negative'
        ELSE 'neutral'
    END AS sentiment
FROM (
    SELECT 
        text,
        sentiment(text) AS sentiment_score
    FROM file('sentiment_data.txt', 'CSV', 'text string')
)
