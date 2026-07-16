FROM python:3.11-slim
RUN pip install --no-cache-dir kafka-python==2.0.2
WORKDIR /app
COPY dlq_processor.py .
# Bản kê topic DLQ do control plane sinh (python -m dataplatform.cli write).
# Sửa contract trong metadata/ -> chạy lại generator -> rebuild image này.
COPY dlq_topics.json .
CMD ["python", "-u", "dlq_processor.py"]