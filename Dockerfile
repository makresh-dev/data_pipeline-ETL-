FROM python:3.10-slim

# Install Java 21 (the only supported version on Debian Trixie ARM64)
RUN apt-get update && apt-get install -y openjdk-21-jre && apt-get clean

# Set JAVA_HOME for JDK 21 (ARM64)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Create app directory
WORKDIR /app

# Copy project files
COPY requirements.txt .
COPY src/ /app/src/
COPY data/ /app/data/


# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create output directory
RUN mkdir -p /app/output

# Run pipeline
CMD ["python", "src/pipeline.py"]
