from kafka import KafkaProducer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import csv

# 3 topics for sentiment analysis POSITIVE, NEGATIVE AND NEUTRAL


producer = KafkaProducer(bootstrap_servers='localhost:9092')
analyzer = SentimentIntensityAnalyzer()

with open("twitter_dataset.csv","r",) as dataset:
    reader=csv.reader(dataset)
    
    for row in reader:
        row[2]=row[2].replace('\n','')
 
        scores=analyzer.polarity_scores(row[2])
        date_time=row[5].split()
        
        data_row=[row[0],row[1],row[2],row[3],row[4],date_time[0],date_time[-1],str(scores['compound'])]
        msg=",".join(data_row).encode("utf-8")
        
        if scores['compound']>=0.05:
            
            producer.send("positive",msg)
            # msg.append ("positive")
            
        elif scores['compound']<=-0.05:
            producer.send("negative",msg)
            # msg.append ("negative")

        else:
            producer.send("neutral" ,msg)
            # data_row.append ("neutral")

        print(data_row)
        print()
        producer.flush()

