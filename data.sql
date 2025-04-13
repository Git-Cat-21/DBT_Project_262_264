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


-- Updated table for tweet volume statistics
CREATE TABLE tweet_volume (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    tweet_count BIGINT,
    processing_time TIMESTAMP,
    PRIMARY KEY (window_start, window_end)
);

-- Updated table for average likes statistics
CREATE TABLE average_likes (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_likes DOUBLE PRECISION,
    processing_time TIMESTAMP,
    PRIMARY KEY (window_start, window_end)
);

-- Updated table for trending words
CREATE TABLE trending_words (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    word VARCHAR(100),
    word_count BIGINT,
    processing_time TIMESTAMP,
    PRIMARY KEY (window_start, window_end, word)
);


SELECT * FROM twitter_sentiments;




-- Table for Query 1: Sentiment Analytics (Average Sentiment Score)
CREATE TABLE sentiment_analytics (
    id SERIAL PRIMARY KEY,
    avg_sentiment_score FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Query 2: Sentiment Count (Positive, Negative, Neutral)
CREATE TABLE sentiment_count (
    id SERIAL PRIMARY KEY,
    sentiment_type VARCHAR(20),
    count INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Query 3: Sentiment Trend Over Time (Per Hour)
CREATE TABLE sentiment_trend (
    id SERIAL PRIMARY KEY,
    hour INT,
    sentiment_type VARCHAR(20),
    count INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


SELECT * FROM   tweets ;

SELECT * FROM  sentiment_summary;

SELECT * FROM  popular_users;

SELECT * FROM  hourly_sentiment;