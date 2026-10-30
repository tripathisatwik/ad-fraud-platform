
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/analytics

RUN pip install --no-cache-dir "Flask>=3.0,<4.0" "gunicorn>=22.0,<24.0"

COPY analytics/app.py ./app.py
COPY analytics/templates ./templates
COPY analytics/static ./static

EXPOSE 8502

CMD ["gunicorn", "--bind", "0.0.0.0:8502", "--workers", "1", "--threads", "4", "--timeout", "30", "app:app"]