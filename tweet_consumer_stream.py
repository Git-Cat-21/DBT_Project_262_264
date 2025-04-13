import pandas as pd
import numpy as np
import time
import schedule
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import psycopg2
from psycopg2 import sql

# Download VADER lexicon
nltk.download('vader_lexicon')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Twitter Sentiment Analysis") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

# Define schema for the Twitter data
schema = StructType([
    StructField("Tweet_ID", StringType(), True),
    StructField("Username", StringType(), True),
    StructField("Text", StringType(), True),
    StructField("Retweets", StringType(), True),
    StructField("Likes", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("compound_score", FloatType(), True),
    StructField("sentiment", StringType(), True)
])

# Initialize VADER sentiment analyzer
sid = SentimentIntensityAnalyzer()

# UDF for sentiment analysis
def analyze_sentiment(text):
    if text is None:
        return 0.0
    return sid.polarity_scores(text)['compound']

def get_sentiment_label(compound_score):
    if compound_score >= 0.05:
        return 'POSITIVE'
    elif compound_score <= -0.05:
        return 'NEGATIVE'
    else:
        return 'NEUTRAL'

# Register UDFs
sentiment_udf = udf(analyze_sentiment, FloatType())
sentiment_label_udf = udf(get_sentiment_label, StringType())

# PostgreSQL connection parameters
db_params = {
    "host": "localhost",
    "database": "twitterdb",
    "user": "user",
    "password": "password"
}

def create_tables():
    """Create tables with batch tracking columns"""
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Tweets table (unchanged)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tweets (
            tweet_id TEXT PRIMARY KEY,
            username TEXT,
            text TEXT,
            retweets INTEGER,
            likes INTEGER,
            timestamp TIMESTAMP,
            compound_score FLOAT,
            sentiment TEXT
        )
        ''')
        
        # Modified: Sentiment summary now includes batch_id
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sentiment_summary (
            id SERIAL PRIMARY KEY,
            batch_id TEXT,  -- New: Unique identifier for each batch run
            sentiment TEXT,
            count INTEGER,
            percentage FLOAT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        
        
        # Modified: Popular users with batch_id
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS popular_users (
            id SERIAL PRIMARY KEY,
            batch_id TEXT,  -- New
            username TEXT,
            avg_sentiment FLOAT,
            total_tweets INTEGER,
            total_engagement INTEGER,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # New: Table for hourly trends (if using the 3rd query)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS hourly_sentiment (
            id SERIAL PRIMARY KEY,
            batch_id TEXT,
            hour TIMESTAMP,
            sentiment TEXT,
            count INTEGER,
            avg_score FLOAT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            
def simulate_real_time_data():
    """Read a batch of data from the CSV to simulate streaming"""
    # In a real system, you would connect to Twitter API
    # For simulation, we'll read from CSV in batches
    try:
        # Read the full dataset once
        if not hasattr(simulate_real_time_data, "full_data"):
            simulate_real_time_data.full_data = pd.read_csv("twitter_dataset.csv")
            simulate_real_time_data.index = 0
            simulate_real_time_data.batch_size = 100  # Process 50 tweets per batch
        
        # Get the next batch
        if simulate_real_time_data.index >= len(simulate_real_time_data.full_data):
            simulate_real_time_data.index = 0  # Reset to beginning to simulate continuous stream
            
        end_idx = min(simulate_real_time_data.index + simulate_real_time_data.batch_size, 
                      len(simulate_real_time_data.full_data))
        batch = simulate_real_time_data.full_data.iloc[simulate_real_time_data.index:end_idx].copy()
        simulate_real_time_data.index = end_idx
        
        # Add some randomness to simulate real-time changes
        batch['Retweets'] = batch['Retweets'].apply(lambda x: max(0, int(x) + np.random.randint(-5, 10)))
        batch['Likes'] = batch['Likes'].apply(lambda x: max(0, int(x) + np.random.randint(-10, 20)))
        
        # Update timestamp to current time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        batch['Timestamp'] = current_time
        
        return batch
    except Exception as e:
        print(f"Error in simulating data: {e}")
        return pd.DataFrame(columns=['Tweet_ID', 'Username', 'Text', 'Retweets', 'Likes', 'Timestamp'])

def process_batch():
    """Process a batch of Twitter data"""
    # Get simulated data
    batch_df = simulate_real_time_data()
    if batch_df.empty:
        print("No data to process")
        return
    
    # Convert pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(batch_df)
    
    # Apply sentiment analysis
    with_sentiment = spark_df.withColumn("compound_score", sentiment_udf(spark_df["Text"])) \
                             .withColumn("sentiment", sentiment_label_udf("compound_score"))
    
    # Register as a temporary view for SQL queries
    with_sentiment.createOrReplaceTempView("tweets")
    
    # Save processed data to PostgreSQL
    save_to_postgres(with_sentiment)
    
    # Execute Spark SQL Queries
    run_spark_sql_queries()
    
    print(f"Processed batch of {batch_df.shape[0]} tweets at {datetime.now()}")

def save_to_postgres(df):
    """Save the processed DataFrame to PostgreSQL"""
    try:
        # Convert spark dataframe to pandas for easier insertion
        pandas_df = df.toPandas()
        
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Insert each row into the tweets table
        for _, row in pandas_df.iterrows():
            cursor.execute(
                """
                INSERT INTO tweets (tweet_id, username, text, retweets, likes, timestamp, compound_score, sentiment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tweet_id) 
                DO UPDATE SET 
                    retweets = EXCLUDED.retweets,
                    likes = EXCLUDED.likes,
                    timestamp = EXCLUDED.timestamp,
                    compound_score = EXCLUDED.compound_score,
                    sentiment = EXCLUDED.sentiment
                """,
                (
                    row['Tweet_ID'],
                    row['Username'],
                    row['Text'],
                    int(row['Retweets']),
                    int(row['Likes']),
                    row['Timestamp'],
                    float(row['compound_score']),
                    row['sentiment']
                )
            )
        
        conn.commit()
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()

def run_spark_sql_queries():
    """Run the three Spark SQL queries and store results in PostgreSQL"""
    try:
        # Query 1: Sentiment distribution
        sentiment_distribution = spark.sql("""
            SELECT 
                sentiment,
                COUNT(*) as count,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tweets) as percentage
            FROM 
                tweets
            GROUP BY 
                sentiment
            ORDER BY 
                count DESC
        """)
        
        # Query 2: Popular users with their sentiment analysis
        popular_users = spark.sql("""
            SELECT 
                Username,
                AVG(compound_score) as avg_sentiment,
                COUNT(*) as total_tweets,
                SUM(CAST(Retweets AS INT) + CAST(Likes AS INT)) as total_engagement
            FROM 
                tweets
            GROUP BY 
                Username
            ORDER BY 
                total_engagement DESC
            LIMIT 10
        """)
        
        # Query 3: Recent sentiment trends over time
        # In a real application, you'd have actual timestamps to work with
        sentiment_trend = spark.sql("""
            SELECT 
                sentiment,
                COUNT(*) as count
            FROM 
                tweets
            GROUP BY 
                sentiment
        """)
        
        # Store query results in PostgreSQL
        store_query_results(sentiment_distribution, popular_users)
        
    except Exception as e:
        print(f"Error in Spark SQL queries: {e}")

def store_query_results(sentiment_distribution, popular_users, hourly_sentiment=None):
    """Store results without deleting old records"""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Generate a unique batch ID
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Insert sentiment distribution with batch_id
        sentiment_df = sentiment_distribution.toPandas()
        for _, row in sentiment_df.iterrows():
            cursor.execute(
                """
                INSERT INTO sentiment_summary 
                (batch_id, sentiment, count, percentage)
                VALUES (%s, %s, %s, %s)
                """,
                (batch_id, row['sentiment'], int(row['count']), float(row['percentage']))
            )
        
        # Insert popular users with batch_id
        users_df = popular_users.toPandas()
        for _, row in users_df.iterrows():
            cursor.execute(
                """
                INSERT INTO popular_users 
                (batch_id, username, avg_sentiment, total_tweets, total_engagement)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (batch_id, row['Username'], float(row['avg_sentiment']), 
                int(row['total_tweets']), int(row['total_engagement']))
            )
        
        # Insert hourly trends if available (from 3rd query)
        if hourly_sentiment:
            hourly_df = hourly_sentiment.toPandas()
            for _, row in hourly_df.iterrows():
                cursor.execute(
                    """
                    INSERT INTO hourly_sentiment 
                    (batch_id, hour, sentiment, count, avg_score)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (batch_id, row['hour'], row['sentiment'], 
                     int(row['count']), float(row['avg_score']))
                )
        
        conn.commit()
    except Exception as e:
        print(f"Error storing query results: {e}")
    finally:
        if conn:
            conn.close()
            
            
def main():
    """Main function to run the Twitter sentiment analysis pipeline"""
    print("Starting Twitter sentiment analysis pipeline...")
    
    # Create database tables if they don't exist
    create_tables()
    
    # Process batches on a schedule to simulate real-time
    schedule.every(20).seconds.do(process_batch)
    
    # Run the first batch immediately
    process_batch()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()