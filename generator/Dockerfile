FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

# -u: unbuffered stdout, log hiện ngay trong `docker compose logs`
CMD ["python", "-u", "main.py"]
