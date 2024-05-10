from kafka import KafkaConsumer
import json
import logging
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_server = 'kafka:9092'
mongo_host = 'host.docker.internal'  # Connect to MongoDB running on the host machine

# Connect to MongoDB using host.docker.internal
mongo_client = MongoClient(f'mongodb://{mongo_host}:27017')
db = mongo_client['twitter_db']
collection = db['twitter_collection']

consumer = KafkaConsumer(
    'twitter_data',
    bootstrap_servers=kafka_server,
    auto_offset_reset='earliest',
    api_version=(0, 11, 5),
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    logger.info(f"Received message: {message.value}")
    
    # Store the message in MongoDB
    data = message.value
    collection.insert_one(data)
    logger.info("Data stored in MongoDB")

    # Process the message as needed
    # For now, we're just logging it
    print(f"Received message: {message.value}")
