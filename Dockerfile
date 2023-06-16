# Start from a base image with Alpine
FROM python:3.10-alpine

# Set the working directory in the Docker image to /app
WORKDIR /app

# Install the PostgreSQL client
RUN apk add --no-cache postgresql-client

# Copy the requirements.txt file from your host to the Docker image
COPY requirements.txt .

# Install the Python packages listed in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the files from your host to the Docker image
COPY . /app

# Run the scheduler
CMD ["python", "backup.py", "schedule"]
