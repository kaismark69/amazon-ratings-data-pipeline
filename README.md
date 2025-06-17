# Data Engineering Portfolio Project – Development Phase

## What the System Does

This project implements a batch-processing data pipeline for Amazon product ratings.  
It consists of four Dockerized microservices that work together to:

1. Ingest monthly rating data from the large CSV file.
2. Store that data in a PostgreSQL database.
3. Preprocess the data using PySpark (remove nulls & duplicates, compute average rating per product).
4. Deliver the results via a Flask API as downloadable CSV file.

The system is designed to be scalable, reproducible, and modular, each part runs as an isolated container using Docker Compose.

## How to Run the Project

### 1. Build and Start Services
```bash
docker-compose build     # build the images (only needed once or when code changes)
docker-compose up -d     # start DB and delivery services in background
```

### 2. Run the Ingestion Service

To ingest data, use the following command: 

```bash
docker-compose run --rm ingestion python main.py <month>
```
Replace <month> with a number from 1 (January) to 12 (December) to specify which month's data should be processed.

For example, to ingest data for July, use:
```bash
docker-compose run --rm ingestion python main.py 7
``` 
This command:
- Loads rating entries for the specified month from the original dataset
- Saves them into the PostgreSQL database as a table named `ratings_month_7`

### 3. Run the Preprocessing Service

Use the same logic as the ingestion step to specify the month you want to process.

For example, to preprocess the data for July (month 7), run:

```bash
docker-compose run --rm preprocessing python main.py 7
```

This command:

- Reads the data from the PostgreSQL table ratings_month_7
- Cleans it by removing nulls and duplicates
- Aggregates the data (average rating per product)
- Saves the result to `/output/aggregated_ratings_month_7.csv`, which will be served by the delivery microservice

### 4. Access the Delivery Service

The delivery microservice runs a Flask API that serves the aggregated results as downloadable CSV files.

Once ingestion and preprocessing for a given month (for example 7 for July) are complete, you can access the result in your browser at:
http://localhost:5000/data/7

This will:

- Look for the file `/output/aggregated_ratings_month_7.csv`
- If the file exists, return it as a CSV download
- If the file is not found, respond with a 404 error
