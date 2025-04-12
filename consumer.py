from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime



consumer = KafkaConsumer( "positive", "negative", "neutral", bootstrap_servers=['localhost:9092'],
                         auto_offset_reset="earliest")
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

connection = None 
try:
    connection = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor=connection.cursor()
    print("Connection successful!")
    
    for message in consumer:
        tweet_data = message.value.decode('utf-8').split(",")
        if tweet_data[0]=="Tweet_ID":
            continue
        
        date_obj = datetime.strptime(tweet_data[5], "%Y-%m-%d").date()
        time_obj = datetime.strptime(tweet_data[6], "%H:%M:%S").time()
        
        data_row=[int(tweet_data[0]),tweet_data[1],tweet_data[2],int(tweet_data[3]),int(tweet_data[4]), date_obj,time_obj,float(tweet_data[7])]
        
        if message.topic=="positive":
            cursor.execute("INSERT INTO positive VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",data_row)
            
        elif message.topic=="negative":
            cursor.execute("INSERT INTO negative VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",data_row)

        else:
            cursor.execute("INSERT INTO neutral VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",data_row)
        
        connection.commit()

except Exception as e:
    print("An error occurred while connecting to the database:", e)
    
finally:
    if connection:
        connection.close()
