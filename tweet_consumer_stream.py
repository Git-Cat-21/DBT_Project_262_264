import pandas as pd
import numpy as np
import time
import schedule
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import psycopg2
import psutil
import os
from dotenv import load_dotenv

load_dotenv()

class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.cpu_usage = []
        self.memory_usage = []
    
    def start(self):
        self.start_time = time.time()
        self.cpu_usage = []
        self.memory_usage = []
    
    def record(self):
        self.cpu_usage.append(psutil.cpu_percent())
        self.memory_usage.append(psutil.virtual_memory().percent)
    
    def get_metrics(self):
        return {
            "execution_time": time.time() - self.start_time,
            "avg_cpu": sum(self.cpu_usage)/max(1, len(self.cpu_usage)),
            "max_cpu": max(self.cpu_usage) if self.cpu_usage else 0,
            "avg_memory": sum(self.memory_usage)/max(1, len(self.memory_usage)),
            "max_memory": max(self.memory_usage) if self.memory_usage else 0,
            "timestamp": datetime.now().isoformat()
        }

monitor = PerformanceMonitor()

nltk.download('vader_lexicon')
spark = SparkSession.builder \
    .appName("Twitter Sentiment Analysis") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

schema = StructType([
    StructField("Tweet_ID", StringType()),
    StructField("Username", StringType()),
    StructField("Text", StringType()),
    StructField("Retweets", StringType()),
    StructField("Likes", StringType()),
    StructField("Timestamp", StringType())
])

sid = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    return sid.polarity_scores(text or "")['compound']

sentiment_udf = udf(analyze_sentiment, FloatType())
sentiment_label_udf = udf(lambda x: 'POSITIVE' if x >= 0.05 else ('NEGATIVE' if x <= -0.05 else 'NEUTRAL'), StringType())


db_params = {
    "host": "localhost",
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def create_tables():
    """Initialize all required tables with proper constraints"""
    commands = (
        """
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
        """,
        """
        CREATE TABLE IF NOT EXISTS sentiment_summary (
            id SERIAL PRIMARY KEY,
            batch_id TEXT NOT NULL,
            sentiment TEXT NOT NULL,
            count INTEGER,
            percentage FLOAT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(batch_id, sentiment)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS popular_users (
            id SERIAL PRIMARY KEY,
            batch_id TEXT NOT NULL,
            username TEXT NOT NULL,
            avg_sentiment FLOAT,
            total_tweets INTEGER,
            total_engagement INTEGER,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(batch_id, username)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS hourly_sentiment (
            id SERIAL PRIMARY KEY,
            batch_id TEXT NOT NULL,
            hour TIMESTAMP NOT NULL,
            sentiment TEXT NOT NULL,
            count INTEGER,
            avg_score FLOAT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(batch_id, hour, sentiment)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS performance_metrics (
            id SERIAL PRIMARY KEY,
            batch_interval INTEGER NOT NULL,
            execution_time FLOAT NOT NULL,
            avg_cpu FLOAT NOT NULL,
            max_cpu FLOAT NOT NULL,
            avg_memory FLOAT NOT NULL,
            max_memory FLOAT NOT NULL,
            record_count INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL
        )
        """
    )
    
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        for command in commands:
            cursor.execute(command)
        conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()


def simulate_real_time_data():
    """Generate realistic streaming data with random fluctuations"""
    try:
        if not hasattr(simulate_real_time_data, "full_data"):
            simulate_real_time_data.full_data = pd.read_csv("twitter_dataset.csv")
            simulate_real_time_data.index = 0
            simulate_real_time_data.batch_size = 200
        
        if simulate_real_time_data.index >= len(simulate_real_time_data.full_data):
            simulate_real_time_data.index = 0
            
        end_idx = min(simulate_real_time_data.index + simulate_real_time_data.batch_size, 
                      len(simulate_real_time_data.full_data))
        batch = simulate_real_time_data.full_data.iloc[simulate_real_time_data.index:end_idx].copy()
        simulate_real_time_data.index = end_idx
        
        # Simulate real-time fluctuations
        batch['Retweets'] = batch['Retweets'].apply(lambda x: max(0, int(x) + np.random.randint(-5, 10)))
        batch['Likes'] = batch['Likes'].apply(lambda x: max(0, int(x) + np.random.randint(-10, 20)))
        batch['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return batch
    except Exception as e:
        print(f"Data simulation error: {e}")
        return pd.DataFrame(columns=['Tweet_ID', 'Username', 'Text', 'Retweets', 'Likes', 'Timestamp'])

def process_batch():
    """Complete batch processing pipeline with monitoring"""
    monitor.start()
    
    # Data ingestion
    batch_df = simulate_real_time_data()
    if batch_df.empty:
        print("Empty batch - skipping")
        return
    
    monitor.record()
    
    # Spark processing
    spark_df = spark.createDataFrame(batch_df)
    with_sentiment = spark_df.withColumn("compound_score", sentiment_udf(col("Text"))) \
                            .withColumn("sentiment", sentiment_label_udf(col("compound_score")))
    
    monitor.record()
    
    # Data storage
    with_sentiment.createOrReplaceTempView("tweets")
    save_to_postgres(with_sentiment)
    run_spark_sql_queries()
    
    # Performance logging
    metrics = monitor.get_metrics()
    print(f"\nBatch Metrics:")
    print(f"- Processed {batch_df.shape[0]} tweets in {metrics['execution_time']:.2f}s")
    print(f"- CPU: avg={metrics['avg_cpu']:.1f}%, max={metrics['max_cpu']:.1f}%")
    print(f"- Memory: avg={metrics['avg_memory']:.1f}%, max={metrics['max_memory']:.1f}%")
    
    store_performance_metrics(metrics, batch_df.shape[0])

def save_to_postgres(df):
    """Upsert tweet data with conflict handling"""
    insert_query = """
    INSERT INTO tweets (tweet_id, username, text, retweets, likes, timestamp, compound_score, sentiment)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (tweet_id) 
    DO UPDATE SET 
        retweets = EXCLUDED.retweets,
        likes = EXCLUDED.likes,
        timestamp = EXCLUDED.timestamp,
        compound_score = EXCLUDED.compound_score,
        sentiment = EXCLUDED.sentiment
    """
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        for row in df.collect():
            cursor.execute(insert_query, (
                row['Tweet_ID'],
                row['Username'],
                row['Text'],
                int(row['Retweets']),
                int(row['Likes']),
                row['Timestamp'],
                float(row['compound_score']),
                row['sentiment']
            ))
        
        conn.commit()
    except Exception as e:
        print(f"Database write error: {e}")
    finally:
        if conn:
            conn.close()

def run_spark_sql_queries():
    """Execute all analytical queries"""
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    
    try:
        # Query 1: Sentiment distribution
        sentiment_dist = spark.sql("""
            SELECT 
                sentiment,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tweets), 2) as percentage
            FROM tweets
            GROUP BY sentiment
            ORDER BY count DESC
        """)
        
        # Query 2: Popular users
        popular_users = spark.sql("""
            SELECT 
                Username,
                ROUND(AVG(compound_score), 4) as avg_sentiment,
                COUNT(*) as total_tweets,
                SUM(CAST(Retweets AS INT) + CAST(Likes AS INT)) as total_engagement
            FROM tweets
            GROUP BY Username
            ORDER BY total_engagement DESC
            LIMIT 10
        """)
        
        # Query 3: Hourly trends 
        hourly_sentiment = spark.sql("""
            SELECT 
                CAST(DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS TIMESTAMP) as hour,
                sentiment,
                COUNT(*) as count,
                ROUND(AVG(compound_score), 4) as avg_score
            FROM tweets
            GROUP BY DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)), sentiment
            ORDER BY hour, sentiment
        """)
        
        store_query_results(sentiment_dist, popular_users, hourly_sentiment, batch_id)
        
    except Exception as e:
        print(f"Query execution error: {e}")

def store_query_results(sentiment_dist, popular_users, hourly_sentiment, batch_id):
    """Store all analytical results"""
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Sentiment distribution
        for row in sentiment_dist.collect():
            cursor.execute("""
                INSERT INTO sentiment_summary (batch_id, sentiment, count, percentage)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (batch_id, sentiment) DO UPDATE SET
                    count = EXCLUDED.count,
                    percentage = EXCLUDED.percentage
                """,
                (batch_id, row['sentiment'], row['count'], row['percentage'])
            )
        
        # Popular users
        for row in popular_users.collect():
            cursor.execute("""
                INSERT INTO popular_users (batch_id, username, avg_sentiment, total_tweets, total_engagement)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, username) DO UPDATE SET
                    avg_sentiment = EXCLUDED.avg_sentiment,
                    total_tweets = EXCLUDED.total_tweets,
                    total_engagement = EXCLUDED.total_engagement
                """,
                (batch_id, row['Username'], row['avg_sentiment'], row['total_tweets'], row['total_engagement'])
            )
        
        # Hourly sentiment 
        for row in hourly_sentiment.collect():
            cursor.execute("""
                INSERT INTO hourly_sentiment (batch_id, hour, sentiment, count, avg_score)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, hour, sentiment) DO UPDATE SET
                    count = EXCLUDED.count,
                    avg_score = EXCLUDED.avg_score
                """,
                (batch_id, row['hour'], row['sentiment'], row['count'], row['avg_score'])
            )
        
        conn.commit()
    except Exception as e:
        print(f"Results storage error: {e}")
    finally:
        if conn:
            conn.close()

def store_performance_metrics(metrics, record_count):
    """Record system performance metrics"""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO performance_metrics (
                batch_interval, execution_time, avg_cpu, max_cpu,
                avg_memory, max_memory, record_count, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (20,  # Adjust for 30-second batches when comparing
             metrics['execution_time'],
             metrics['avg_cpu'],
             metrics['max_cpu'],
             metrics['avg_memory'],
             metrics['max_memory'],
             record_count,
             metrics['timestamp'])
        )
        
        conn.commit()
    except Exception as e:
        print(f"Metrics storage error: {e}")
    finally:
        if conn:
            conn.close()

def main():
    """Run the streaming pipeline"""
    print("Initializing Twitter Sentiment Analysis Pipeline")
    create_tables()
    
    # Process first batch immediately
    process_batch()
    
    # Schedule subsequent batches
    schedule.every(20).seconds.do(process_batch)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()