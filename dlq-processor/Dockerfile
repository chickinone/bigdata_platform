FROM python:3.11-slim
RUN pip install --no-cache-dir kafka-python==2.0.2
WORKDIR /app
COPY dlq_processor.py .
CMD ["python", "-u", "dlq_processor.py"]