FROM python:3.13.0a1-slim-bookworm

# Setup working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Copy source code
COPY . /app

# Dummy command
CMD [ "sh" ]