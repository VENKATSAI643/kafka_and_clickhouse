FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY producer.py .

# Create the dead-letter log file (mounted as volume)
RUN touch /app/failed_messages.jsonl

CMD ["python", "-u", "producer.py"]
