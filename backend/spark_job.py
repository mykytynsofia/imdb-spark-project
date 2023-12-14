from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("CSV to BigQuery") \
.getOrCreate()

csv_path = "gs://imdb-dataframes/title.ratings.tsv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

bq_project_id = "mlops-398308"
bq_dataset_id = "imdb"
bq_table_id = "ratings"

df.write.format("bigquery").option("temporaryGcsBucket", "imdb-dataframes").option("table", f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}").mode("overwrite").save()

spark.stop()