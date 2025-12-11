FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

COPY watcher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# moduli condivisi
COPY db.py .
COPY loraitalia.py .

# codice watcher
COPY watcher/watcher.py .

CMD ["python", "watcher.py"]
