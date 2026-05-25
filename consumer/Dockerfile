FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

RUN useradd --create-home --shell /bin/bash appuser
USER appuser

CMD ["python", "-u", "consumer.py"]
