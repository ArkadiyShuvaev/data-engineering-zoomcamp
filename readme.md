A data engineering course.

<img src="./img/cbb7c6dd-7f8a-4565-b2fd-96abf31632d0.webp" alt="course picture" height="300">

## Course Description

### How to test the data ingestion script
To ingest data from a source and load it into a target, you can use the following command:
```bash
python ./data/data_ingestion.py --file_url https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet --db_table_name green_tripdata_2019_09 --db_name ny_taxi --db_host_name db --db_port 5432 --db_user_name postgres --db_password P@ssw0rd!
```

### How to build an image to ingest data from a source and load it into a target
