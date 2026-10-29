FROM python:3.11-slim

WORKDIR /opt/app

RUN pip install --no-cache-dir fastapi uvicorn boto3

COPY api/app.py ./app.py

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]