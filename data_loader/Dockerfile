# The Dockerfile builds a container and executes the script to feed database with downloaded database
FROM python:3.12

# Set the working directory in the container
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
COPY ./data_loader/data_loader.py /app/data_loader.py

ENV PYTHONUNBUFFERED=1

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "data_loader.py", "--file_url", "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet", "--db_table_name", "green_tripdata_2019_09", "--db_name", "ny_taxi", "--db_host_name", "db", "--db_port", "5432", "--db_user_name", "postgres", "--db_password", "P@ssw0rd!"]