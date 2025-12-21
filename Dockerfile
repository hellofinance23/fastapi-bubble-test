FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files and modules
COPY main.py .
COPY routes/ ./routes/
COPY cleaning/ ./cleaning/
COPY utils/ ./utils/
COPY validations/ ./validations/

# Use Railway's PORT environment variable
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}