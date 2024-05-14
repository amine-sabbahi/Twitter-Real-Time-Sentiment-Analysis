#!/bin/bash
# Sleep for a short period to ensure the Spark master is fully up
sleep 10

# Install Python dependencies
pip install -r /app/data_consumer/requirements.txt

# Optionally, start the Spark session within the script if needed
# This is just an example. Adjust based on your actual Spark session needs.
# spark-submit --master spark://localhost:7077 --class your.main.Class your-spark-application.jar

# Run the consumer script
python /app/data_consumer/kafka_consumer.py
