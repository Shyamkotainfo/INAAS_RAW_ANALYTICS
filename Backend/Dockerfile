FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install Java + curl (for pyspark)
RUN apt-get update && apt-get install -y \
    curl \
    openjdk-21-jre \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements first
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY . .

EXPOSE 8000

CMD ["python", "main.py", "--mode", "api", "--port", "8000"]
