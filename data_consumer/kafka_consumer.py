from kafka import KafkaConsumer
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_server = 'kafka:9092'

consumer = KafkaConsumer(
    'twitter_data',
    bootstrap_servers=kafka_server,
    auto_offset_reset='earliest',
    api_version=(0,11,5),
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    logger.info(f"Received message: {message.value}")
    # Process the message as needed
    # For now, we're just logging it
    print(f"Received message: {message.value}")
