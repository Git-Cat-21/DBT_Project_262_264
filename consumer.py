from kafka import KafkaConsumer

consumer = KafkaConsumer( "positive", "negative", "neutral", bootstrap_servers=['localhost:9092'],
                         auto_offset_reset="earliest")

for message in consumer:
    tweet_data = message.value.decode('utf-8').split(",")
    print(f"[{message.topic}] {tweet_data}")
    
    print()