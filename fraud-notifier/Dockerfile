FROM python:3.11-slim
RUN pip install --no-cache-dir kafka-python==2.0.2
WORKDIR /app
COPY fraud_notifier.py .
CMD ["python", "-u", "fraud_notifier.py"]