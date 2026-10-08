FROM python:3.12-slim

WORKDIR /opt/kafka-apps

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY kafka/inference_consumer.py .

CMD ["python", "inference_consumer.py"]