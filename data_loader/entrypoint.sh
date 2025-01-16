#!/bin/bash

# Check if all required environment variables are provided
if [ -z "$FILE_URL" ]; then
  echo "FILE_URL is not set"
  exit 1
fi

if [ -z "$DB_TABLE_NAME" ]; then
  echo "DB_TABLE_NAME is not set"
  exit 1
fi

if [ -z "$DB_NAME" ]; then
  echo "DB_NAME is not set"
  exit 1
fi

if [ -z "$DB_HOST_NAME" ]; then
  echo "DB_HOST_NAME is not set"
  exit 1
fi

if [ -z "$DB_PORT" ]; then
  echo "DB_PORT is not set"
  exit 1
fi

if [ -z "$DB_USER_NAME" ]; then
  echo "DB_USER_NAME is not set"
  exit 1
fi

if [ -z "$DB_PASSWORD" ]; then
  echo "DB_PASSWORD is not set"
  exit 1
fi

if [ -z "$DB_TAXI_ZONES_TABLE_NAME" ]; then
  echo "DB_TAXI_ZONES_TABLE_NAME is not set"
  exit 1
fi

if [ -z "$FILE_TAXI_ZONES_URL" ]; then
  echo "FILE_TAXI_ZONES_URL is not set"
  exit 1
fi

# Execute the data_loader.py script with the provided environment variables
python data_loader.py --file_url "$FILE_URL" --db_table_name "$DB_TABLE_NAME" --db_name "$DB_NAME" --db_host_name "$DB_HOST_NAME" --db_port "$DB_PORT" --db_user_name "$DB_USER_NAME" --db_password "$DB_PASSWORD" --db_taxi_zones_table_name "$DB_TAXI_ZONES_TABLE_NAME" --file_taxi_zones_url "$FILE_TAXI_ZONES_URL"
