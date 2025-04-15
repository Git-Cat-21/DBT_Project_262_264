    #record count
SELECT 
  (SELECT COUNT(*) FROM positive) AS positive_count,
  (SELECT COUNT(*) FROM negative) AS negative_count,
  (SELECT COUNT(*) FROM neutral) AS neutral_count;


#engagement analysis
SELECT 
  'positive' AS sentiment,
  AVG(likes) AS avg_likes,
  AVG(retweets) AS avg_retweets,
  SUM(likes) AS total_likes,
  SUM(retweets) AS total_retweets
FROM positive
UNION ALL
SELECT 
  'negative',
  AVG(likes), 
  AVG(retweets),
  SUM(likes),
  SUM(retweets)
FROM negative
UNION ALL
SELECT 
  'neutral',
  AVG(likes), 
  AVG(retweets),
  SUM(likes),
  SUM(retweets)
FROM neutral;


#Sentiment Score Accuracy Check(output:should not return rows if it does then it means its missclassified)
-- Are there any positive tweets with senti_score <= 0?
SELECT * FROM positive WHERE senti_score <= 0;

-- Any neutral tweets that should not be neutral?
SELECT * FROM neutral WHERE senti_score > 0.1 OR senti_score < -0.1;

-- Any negative tweets with positive score?
SELECT * FROM negative WHERE senti_score >= 0;

#Temporal Patterns
SELECT 
  date,
  COUNT(*) AS tweet_count,
  AVG(senti_score) AS avg_score
FROM positive
GROUP BY date
ORDER BY date;

#Most Active Users
SELECT user_name, COUNT(*) AS tweet_count
FROM positive
GROUP BY user_name
ORDER BY tweet_count DESC
LIMIT 5;


#top performing tweets
SELECT tweet_id, user_name, tweet, likes
FROM positive
ORDER BY likes DESC
LIMIT 5;