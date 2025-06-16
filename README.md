# 🐦 TweetTide: The Social Pipeline 🌊

This project implements a comprehensive data processing pipeline for Twitter data using both batch and stream processing methodologies.

## Overview

The system leverages Apache Kafka for message queuing and Apache Spark for distributed data processing. The pipeline consists of:

1. **Data Collection**: Twitter data is ingested into the system
2. **Processing Pipeline**:
    - **Batch Processing**: Historical data is processed in batches
    - **Stream Processing**: Real-time data is processed as it arrives
3. **Implementation Note**: Since Twitter API requires payment, we simulated the streaming data by using Python's time module to add a 1-second delay between messages, creating a realistic stream simulation without actual API access


<details>
<summary> How to set up Kafka?</summary>
<br>

```bash
docker compose -f docker-compose.yml up -d  
```

```bash
docker exec -it <kafka_conatiner_id> /bin/sh
```

```bash
cd opt/kafka_2.13-2.8.1/bin
```

## Create a kafka topic 
```bash
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic quickstart
```

## Terminal 1 
```bash
kafka-console-producer.sh --topic quickstart --bootstrap-server localhost:9092
```

## Terminal 2
```bash
kafka-console-consumer.sh --topic quickstart --from-beginning --bootstrap-server localhost:9092
```
</details>

<details>
<summary> How to set up Spark?</summary>
<br>

```bash
wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
```

```bash
tar -xvzf spark-3.5.5-bin-hadoop3.tgz
mv spark-3.5.5-bin-hadoop3 ~/spark
```

```bash
nano ~/.bashrc
```

```bash
export SPARK_HOME=~/spark
export PATH=$SPARK_HOME/bin:$PATH
```

```bash
source ~/.bashrc
```

## Verify Spark Installation
```bash
pyspark
```
</details>

<details>
<summary> How to install postgresql</summary>
<br>

```bash
sudo apt install postgresql
```
</details>

## Create a virtual environment
```bash
python3 -m venv venv
source ./venv/bin/activate
```

## Install Dependencies
```bash
pip3 install -r requirements.txt
```
## Run the data.sql script

<details>
<summary>PostgreSQL Database Setup Instructions for VSC</summary>

#### 1. Install Database Client
Recommended extension for VS Code:
- **Name**: Database Client JDBC
- **VS Marketplace Link**: [Database Client JDBC](https://marketplace.visualstudio.com/items?itemName=cweijan.dbclient-jdbc)

#### 2. Connect to PostgreSQL Database
Select PostgreSQL and use credentials to connect:

```properties
Host: localhost 
Port: 5432 
Username: your_username
Password: your_password
```

#### 3. Run the data.sql script

</details>

----

### VADER Lexicon Checker for NLTK Sentiment Analysis
```bash
python3 vader_download.py vader_download.py
```

## Processing Modes

### Batch Processing

### Terminal 1
```bash
python3 consumer.py
```

### Terminal 2
```bash
python3 producer.py
```

### Stream Processing

```bash
python3 tweet_consumer_stream.py
```
