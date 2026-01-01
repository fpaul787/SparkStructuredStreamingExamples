# Databricks notebook source
# Kafka configuration
username = dbutils.secrets.get(scope="structured_streaming", key="confluent_api_key")
password = dbutils.secrets.get(scope="structured_streaming", key="confluent_api_secret")
kafka_bootstrap_servers = dbutils.secrets.get(scope="structured_streaming", key="confluent_bootstrap_servers")
kafka_topic = 'topic_car_purchases'

# Kafka Consumer
kafka_config = {
    'subscribe': kafka_topic,
    'kafka.bootstrap.servers': kafka_bootstrap_servers,
    'kafka.security.protocol': 'SASL_SSL',
    'startingOffsets': 'earliest',
    'kafka.sasl.mechanism': 'PLAIN',
    'failOnDataLoss': 'false',
    'kafka.ssl.endpoint.identification.algorithm': 'https',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";'
}


# COMMAND ----------

# Define schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

car_purchase_schema = StructType([
    StructField("transactionId", StringType(), nullable=False),
    StructField("customerId", StringType(), nullable=False),
    StructField("customerName", StringType(), nullable=False),
    StructField("customerEmail", StringType(), nullable=True),
    StructField("vin", StringType(), nullable=False),
    StructField("make", StringType(), nullable=False),
    StructField("model", StringType(), nullable=False),
    StructField("year", IntegerType(), nullable=False),
    StructField("color", StringType(), nullable=True),
    StructField("purchasePrice", DecimalType(precision=10, scale=2), nullable=False),
    StructField("dealershipId", StringType(), nullable=False),
    StructField("dealershipName", StringType(), nullable=True),
    StructField("paymentMethod", StringType(), nullable=False),
    StructField("purchaseDate", TimestampType(), nullable=False),
    StructField("salesRepId", StringType(), nullable=True)
])

# COMMAND ----------

# Read stream from Kafka
df = (spark.readStream
  .format("kafka")
  .options(**kafka_config)
  .load())

# COMMAND ----------

from pyspark.sql.functions import col, from_json
parsed_df = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(
        col("key").alias("transactionId_key"),
        from_json(col("value"), car_purchase_schema).alias("purchase")
    ) \
    .select("transactionId_key", "purchase.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continuously processes data
# MAGIC
# MAGIC Databricks does stuff behind the scenes with the `display` function
# MAGIC that starts the streaming query and attaching a temporary sink.
# MAGIC
# MAGIC So display(df) is conceptually similar to 
# MAGIC ```
# MAGIC df.writeStream
# MAGIC   .format("memory")      # or internal sink
# MAGIC   .trigger(processingTime="5 seconds")
# MAGIC   .start()
# MAGIC
# MAGIC ```

# COMMAND ----------

display(parsed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### trigger (once or availableNow)
# MAGIC
# MAGIC The query processes all available data and then stops.

# COMMAND ----------

query = (
    parsed_df.writeStream
        .format("console")
        .trigger(availableNow=True)
        .start()
)

# COMMAND ----------

display(parsed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ProcessingTime (Fixed interval micro-batching)
# MAGIC
# MAGIC Queries runs at fixed intervals.

# COMMAND ----------

query = (
    parsed_df.writeStream
        .format("console")
        .trigger(processingTime='5 seconds')
        .start()
)
