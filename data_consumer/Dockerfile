FROM python:3.8

WORKDIR /app

COPY kafka_consumer.py /app
COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "kafka_consumer.py"]