# Use official Python 3.10 base image
FROM python:3.10

# Set working directory inside the container
WORKDIR /usr/src

# Install system dependencies
RUN apt-get update && apt-get install -y librdkafka-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY ../requirements.txt .

# Upgrade pip and install Python packages
RUN python -m pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH so the container can run Python scripts properly
ENV PYTHONPATH=/usr/src

RUN rm -f requirements.txt

CMD ["tail", "-f", "/dev/null"]
