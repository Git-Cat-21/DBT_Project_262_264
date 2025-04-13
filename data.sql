-- Active: 1744459583397@@127.0.0.1@5432@twitterdb

-- Tweet_ID,Username,Text,Retweets,Likes,Date, Time

CREATE TABLE positive(
    tweet_id INT ,
    user_name VARCHAR(40),
    tweet TEXT,
    retweets INT,
    likes INT,
    date DATE,
    time TIME,
    senti_score FLOAT
);

CREATE TABLE negative(
    tweet_id INT  ,
    user_name VARCHAR(40),
    tweet TEXT,
    retweets INT,
    likes INT,
    date DATE,
    time TIME,
    senti_score FLOAT
);

CREATE TABLE neutral(
    tweet_id INT  ,
    user_name VARCHAR(40),
    tweet TEXT,
    retweets INT,
    likes INT,
    date DATE,
    time TIME,
    senti_score FLOAT
);

SELECT * FROM positive;

SELECT * FROM negative;

SELECT * FROM neutral;


--


SELECT * FROM   tweets ;

SELECT * FROM  sentiment_summary;

SELECT * FROM  popular_users;

SELECT * FROM  hourly_sentiment;

SELECT * FROM performance_metrics;