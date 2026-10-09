FROM python:3.11-slim

WORKDIR /opt/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aws/prediction_sink.py ./prediction_sink.py

CMD ["python", "-u", "prediction_sink.py"]