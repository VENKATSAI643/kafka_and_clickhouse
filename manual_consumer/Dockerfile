FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY manual_consumer.py .

CMD ["python", "-u", "manual_consumer.py"]
