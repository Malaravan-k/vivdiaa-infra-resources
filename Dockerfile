# Use official Python image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install dependencies
RUN pip install --no-cache-dir boto3 requests pandas

# Set the command to run the script
CMD ["python", "county_overview.py"]
