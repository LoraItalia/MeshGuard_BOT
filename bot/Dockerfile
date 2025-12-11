FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

COPY bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# moduli condivisi
COPY db.py .
COPY loraitalia.py .

# codice bot
COPY bot/bot.py .

CMD ["python", "bot.py"]
