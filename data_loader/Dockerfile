# The Dockerfile builds a container and executes the script to feed database with downloaded database
FROM python:3.12

# Declare ARG for build-time variables
ARG FILE_URL
ARG DB_TABLE_NAME
ARG DB_NAME
ARG DB_HOST_NAME
ARG DB_PORT
ARG DB_USER_NAME
ARG DB_PASSWORD
ARG DB_TAXI_ZONES_TABLE_NAME
ARG FILE_TAXI_ZONES_URL

# Assign ARG values to ENVs for runtime
ENV FILE_URL=$FILE_URL
ENV DB_TABLE_NAME=$DB_TABLE_NAME
ENV DB_NAME=$DB_NAME
ENV DB_HOST_NAME=$DB_HOST_NAME
ENV DB_PORT=$DB_PORT
ENV DB_USER_NAME=$DB_USER_NAME
ENV DB_PASSWORD=$DB_PASSWORD
ENV DB_TAXI_ZONES_TABLE_NAME=$DB_TAXI_ZONES_TABLE_NAME
ENV FILE_TAXI_ZONES_URL=$FILE_TAXI_ZONES_URL

# Set the working directory in the container
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
COPY ./data_loader/data_loader.py /app/data_loader.py

# Use a script as the entrypoint to handle dynamic ENV substitution
COPY ./data_loader/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV PYTHONUNBUFFERED=1

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
