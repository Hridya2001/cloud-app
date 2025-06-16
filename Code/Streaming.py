from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("NewsKafkaToPostgres") \
    .config("spark.jars","/home/hridya/Downloads/postgresql-42.2.28.jre7.jar") \  # Update path if needed
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Define the schema of your news data
schema = StructType() \
    .add("title", StringType()) \
    .add("description", StringType()) \
    .add("url", StringType()) \
    .add("publishedAt", StringType())

# Step 3: Read streaming data from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Step 4: Parse JSON from Kafka message value
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# Optional: Filter out null titles (invalid records)
df_filtered = df_parsed.filter(col("title").isNotNull())

# Step 5: Define function to write batch to PostgreSQL RDS
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://imdb-database.cnumq42kaugo.ap-south-1.rds.amazonaws.com:5432/imdb") \
        .option("dbtable", "news_articles") \
        .option("user", "postgresdata") \
        .option("password", "your_password_here") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Step 6: Start the stream
query = df_filtered.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/news-checkpoint/") \
    .start()

query.awaitTermination()
