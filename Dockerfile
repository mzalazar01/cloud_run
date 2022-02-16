
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.9

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
ENV GOOGLE_APPLICATION_CREDENTIALS "sybogames-analytics-dev-2bfad2adb3ac.json"
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN pip install --no-cache-dir -r requirements.txt

CMD uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1
