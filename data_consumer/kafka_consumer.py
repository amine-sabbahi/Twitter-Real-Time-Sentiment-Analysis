from kafka import KafkaConsumer
import json
import logging
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_server = 'kafka:9092'
mongo_host = 'host.docker.internal'  # Connect to MongoDB running on the host machine

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamWithMLPredictions") \
    .getOrCreate()

# Load pre-trained Logistic Regression model
lr_model_path = "/app/data_consumer/lr_model"
lr_model = PipelineModel.load(lr_model_path)

# Connect to MongoDB using host.docker.internal
mongo_client = MongoClient(f'mongodb://{mongo_host}:27017')
db = mongo_client['twitter_db']
collection = db['twitter_collection']

# Define Kafka consumer
consumer = KafkaConsumer(
    'twitter_data',
    bootstrap_servers=kafka_server,
    auto_offset_reset='earliest',
    api_version=(0, 11, 5),
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    try:
        logger.info(f"Received message: {message.value}")

        # Convert message to Spark DataFrame for prediction
        rdd = spark.sparkContext.parallelize([message.value])
        df = spark.read.json(rdd)

        # Make predictions using the Logistic Regression model
        predictions = lr_model.transform(df)
        
        # Extract the necessary fields from the DataFrame
        predictions = predictions.withColumnRenamed('prediction', 'PredictedSentiment')
        predicted_data = predictions.select('TweetID', 'Entity', 'Sentiment', 'TweetContent', 'PredictedSentiment')
        predicted_data_dict = [row.asDict() for row in predicted_data.collect()]

        # Store the predicted message in MongoDB
        if predicted_data_dict:
            collection.insert_many(predicted_data_dict)
            logger.info("Predicted data stored in MongoDB")
        else:
            logger.info("No data to store")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Cleanup Spark session
spark.stop()
