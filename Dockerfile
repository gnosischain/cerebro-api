FROM python:3.11-slim

# Set working directory
WORKDIR /code

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy application code and configuration
COPY ./app /code/app
COPY ./api_config.yaml /code/api_config.yaml

# In a real CI/CD, manifest.json comes from the build artifact. 
# For local dev, we copy it in.
COPY ./manifest.json /code/manifest.json

# Set non-root user for security
RUN useradd -m myuser
USER myuser

# Expose port
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]