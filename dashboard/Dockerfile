FROM python:3.11-slim
WORKDIR /opt/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dashboard/app.py ./app.py
EXPOSE 8501
CMD ["python", "-u", "app.py"]
