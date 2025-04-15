
--  Total Engagement (Likes + Retweets)
SELECT 'Total Engagement (Likes + Retweets)',
    (SELECT SUM(likes + retweets) FROM (
        SELECT likes, retweets FROM positive
        UNION ALL
        SELECT likes, retweets FROM negative
        UNION ALL
        SELECT likes, retweets FROM neutral
    ) AS batch_engagement) AS batch_value,
    (SELECT SUM(likes + retweets) FROM tweets) AS stream_value

UNION ALL

-- Average Sentiment Score
SELECT 'Average Sentiment Score',
    (SELECT AVG(senti_score) FROM (
        SELECT senti_score FROM positive
        UNION ALL
        SELECT senti_score FROM negative
        UNION ALL
        SELECT senti_score FROM neutral
    ) AS batch_score) AS batch_value,
    (SELECT AVG(compound_score) FROM tweets) AS stream_value

UNION ALL


--  Average Tweets Per User
SELECT 'Average Tweets Per User',
    (SELECT COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT user_name), 0) FROM (
        SELECT user_name FROM positive
        UNION ALL
        SELECT user_name FROM negative
        UNION ALL
        SELECT user_name FROM neutral
    ) AS batch_data) AS batch_value,
    (SELECT COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT username), 0) FROM tweets) AS stream_value;


--  Engagement Metrics
SELECT 
    'Average Likes Per Tweet' AS metric,
    (SELECT AVG(likes) FROM (
        SELECT likes FROM positive
        UNION ALL
        SELECT likes FROM negative
        UNION ALL
        SELECT likes FROM neutral
    ) AS batch_likes) AS batch_value,
    (SELECT AVG(likes) FROM tweets) AS stream_value

UNION ALL

SELECT 
    'Average Retweets Per Tweet' AS metric,
    (SELECT AVG(retweets) FROM (
        SELECT retweets FROM positive
        UNION ALL
        SELECT retweets FROM negative
        UNION ALL
        SELECT retweets FROM neutral
    ) AS batch_retweets) AS batch_value,
    (SELECT AVG(retweets) FROM tweets) AS stream_value

UNION ALL

--  Engagement by Sentiment Type
SELECT 
    'Avg Engagement for Positive Tweets' AS metric,
    (SELECT AVG(likes + retweets) FROM positive) AS batch_value,
    (SELECT AVG(likes + retweets) FROM tweets WHERE sentiment = 'positive') AS stream_value

UNION ALL

SELECT 
    'Avg Engagement for Negative Tweets' AS metric,
    (SELECT AVG(likes + retweets) FROM negative) AS batch_value,
    (SELECT AVG(likes + retweets) FROM tweets WHERE sentiment = 'negative') AS stream_value

UNION ALL


-- User Activity Comparison
SELECT 
    'Avg Tweets per Active User' AS metric,
    (SELECT AVG(tweet_count) FROM (
        SELECT user_name, COUNT(*) AS tweet_count FROM (
            SELECT user_name FROM positive
            UNION ALL
            SELECT user_name FROM negative
            UNION ALL
            SELECT user_name FROM neutral
        ) AS batch_users
        GROUP BY user_name
    ) AS user_counts) AS batch_value,
    (SELECT AVG(tweet_count) FROM (
        SELECT username, COUNT(*) AS tweet_count 
        FROM tweets
        GROUP BY username
    ) AS user_counts) AS stream_value
