FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "kafka-python==2.3.0"

COPY producer.py .

CMD ["python", "-u", "producer.py"]
