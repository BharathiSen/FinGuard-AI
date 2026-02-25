FROM python:3.11-slim

WORKDIR /app

# Install pathway and all its dependencies explicitly
RUN pip install --no-cache-dir \
    pathway==0.8.0 \
    beartype \
    numpy>=1.24.0 \
    python-dotenv>=1.0.0 \
    diskcache

# Copy application code
COPY . .

# Create output directory
RUN mkdir -p output

CMD ["python", "pipeline.py"]
