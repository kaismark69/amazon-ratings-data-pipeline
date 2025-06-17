import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import glob
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
# DB config from env
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "storage-db"),
    "port": "5432",
    "dbname": os.environ.get("DB_NAME", "ratings"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "postgres")
}

def main():
    # Get month from command-line args
    month = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    table_name = f"ratings_month_{month}"
    temp_dir = f"/output/tmp_ratings_month_{month}"
    final_path = f"/output/aggregated_ratings_month_{month}.csv"
    try:
        logging.info("Starting Spark session...")
        # Start a Spark session with a PostgreSQL connector (JDBC JAR must be available inside container)
        # JDBC JAR file is needed to connect Spark to PostgreSQL
        spark = SparkSession.builder \
            .appName("Preprocessing Service") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.7.7.jar") \
            .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.7.jar") \
            .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.7.jar") \
            .getOrCreate()
        # Build JDBC connection string to connect to the PostgreSQL database.
        jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        # load the table from PostgreSQL into a Spark DataFrame
        logging.info(f"Reading table from PostgreSQL: {table_name}")
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", DB_CONFIG["user"]) \
            .option("password", DB_CONFIG["password"]) \
            .load()

        logging.info(f"Rows read: {df.count()}")
        #Clean the data: remove rows with missing values and duplicate entries
        df_clean = df.dropna().dropDuplicates()
        logging.info("Performing aggregation...")
        #Aggregate: group the data by product ID and then compute the average rating per each product ID
        aggregated = df_clean.groupBy("productid").agg(avg("rating").alias("avg_rating"))
        logging.info(f"Saving aggregated results to: {final_path}")
        #Save the results as a CSV file to a shared volume called /output
        # coalesce(1) is used to write the output as a single CSV file
        # overwrite mode ensures that the file is replaced if it already exists
        aggregated.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")
        csv_files = glob.glob(f"{temp_dir}/part-*.csv")
        if csv_files:
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(csv_files[0], final_path)
        logging.info("Preprocessing complete.")
        spark.stop()

    except Exception as e:
        logging.error(f"Preprocessing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
