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

## Install Dependencies
```bash
pip3 install -r requirements.txt
```
