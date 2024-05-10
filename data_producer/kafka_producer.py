from kafka import KafkaProducer
import csv
import json
import logging
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_server = 'kafka:9092'
producer = KafkaProducer(bootstrap_servers=kafka_server, api_version=(0,11,5), value_serializer=lambda x: json.dumps(x).encode('utf-8'))

with open('twitter_training.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        message = {
            "TweetID": int(row['Tweet ID']),
            "Entity": row['Entity'],
            "Sentiment": row['Sentiment'],
            "TweetContent": row['Tweet content']
        }
        producer.send('twitter_data', value=message)
        logger.info(f"Produced message: {message}")
        sleep(1)
# Flush the producer to ensure all messages are sent
producer.flush()

# Adding new comment
# Adding second comment