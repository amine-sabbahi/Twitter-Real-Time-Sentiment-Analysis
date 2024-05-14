FROM python:3.8

WORKDIR /app

COPY kafka_producer.py /app
COPY requirements.txt /app
COPY Dataset/twitter_training.csv /app
COPY Dataset/twitter_validation.csv /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "kafka_producer.py"]