FROM python:3.12-slim

WORKDIR /app

# Install dependencies first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY producer.py .

# Run as non-root for security
RUN useradd --create-home --shell /bin/bash appuser
USER appuser

CMD ["python", "-u", "producer.py"]
