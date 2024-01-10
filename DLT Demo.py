# Databricks notebook source
# MAGIC %md
# MAGIC ##Import Libraries

# COMMAND ----------

print("0. Import Libraries")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

print("1. Create DLT in Bronze")

# COMMAND ----------

@dlt.create_table(
  comment="Trip data from NYC taxi yellow databricks datasets",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def 



bronze_yellow():
  return (
    spark.readStream.format("cloudFiles") \
 #     .option("cloudFiles.schemaLocation", "/databricks-datasets/nyctaxi/tripdata/fhv/") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load("/databricks-datasets/nyctaxi/tripdata/yellow/")
  )

# COMMAND ----------

@dlt.create_table(
  comment="Trip data from NYC taxi Green databricks datasets",
  table_properties={
    "myCompanyPipeline.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def bronze_green():
  return (
    spark.readStream.format("cloudFiles") \
 #     .option("cloudFiles.schemaLocation", "/databricks-datasets/nyctaxi/tripdata/fhv/") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load("/databricks-datasets/nyctaxi/tripdata/green/")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

print("2. Clean up data in silver with DLT Expectations")

# COMMAND ----------

print("Establish rules for DLT expectations")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- only use if you have permission to create tables
# MAGIC /*
# MAGIC CREATE OR REPLACE TABLE
# MAGIC   rules
# MAGIC AS SELECT
# MAGIC   col1 AS name,
# MAGIC   col2 AS constraint,
# MAGIC   col3 AS tag
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC   ("VendorID","VendorID IS NOT NULL","validity"),
# MAGIC   ("PULocationID","PULocationID IS NOT NULL","validity"),
# MAGIC   ("DOLocationID","DOLocationID IS NOT NULL","validity")
# MAGIC )
# MAGIC */

# COMMAND ----------

rules_df = spark.createDataFrame(
    [
        (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
    ],
    ["name", "constraint", "tag"]  # add your column names here
)

# COMMAND ----------

print("Define functions for getting DLT expectations from rules tables")

# COMMAND ----------


def get_rules(tag):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  #df_rules = spark.read.table("rules") --- only use if you have permission to create a tabe
  for row in df_rules.filter(col("tag") == tag).collect():
    rules[row['name']] = row['constraint']
  return rules


# COMMAND ----------

print("clean up yellow")

# COMMAND ----------

@dlt.create_table(
  comment="Clean yellow",
  partition_cols=["PULocationID"],
  table_properties={
    "myCompanyPipeline.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all_or_drop(get_rules('validity'))

def silver_yellow():
  df_yellow = dlt.read_stream("bronze_yellow")
  df_yellow = df_yellow.withColumn("silver_datetime", current_timestamp()) 
  df_yellow = df_yellow.withColumn("TripColor", lit("Yellow") )
  df_yellow = df_yellow.select("VendorID", "TripColor",
    "PULocationID",
    "pickup_datetime",
    "DOLocationID",
    "dropoff_datetime",
    "RatecodeID",
    "congestion_surcharge",
    "improvement_surcharge",
    "Extra",
    "_rescued_data"  
  )
  return df_yellow


# COMMAND ----------

print("clean up green")

# COMMAND ----------

@dlt.create_table(
  comment="Clean green",
  partition_cols=["PULocationID"],
  table_properties={
    "myCompanyPipeline.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all_or_drop(get_rules('validity'))

def silver_green():
  df_green = dlt.read_stream("bronze_green")
  df_green = df_green.withColumn("silver_datetime", current_timestamp()) 
  df_green = df_green.withColumn("TripColor", lit("Green") )
  df_green = df_green.withColumn("pickup_datetime", col("lpep_pickup_datetime") )
  df_green = df_green.withColumn("dropoff_datetime", col("Lpep_dropoff_datetime") )
  df_green = df_green.select("VendorID", "TripColor",
    "PULocationID",
    "pickup_datetime",
    "DOLocationID",
    "dropoff_datetime",
    "RatecodeID",
    "congestion_surcharge",
    "improvement_surcharge",
    "Extra",
    "_rescued_data"  
  )
  return df_green

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

print("3. Append to Gold")

# COMMAND ----------

print("combine clean_yellow and clean_green for trips table")

# COMMAND ----------

@dlt.create_table(
  comment="Combine yellow and green to create trips",
  table_properties={
    "myCompanyPipeline.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def gold_trips():
  return dlt.read_stream("silver_green").unionByName(dlt.read_stream("silver_yellow")).withColumn("gold_datetime", current_timestamp()) 

# COMMAND ----------

print("4. create SCD streaming table")

# COMMAND ----------

dlt.create_streaming_table(name="scd_vendorid")

# COMMAND ----------

#Track_history_column_list any changes in hash based on key will be treated as history record.
dlt.apply_changes(
  target = "scd_vendorid",
  source = "gold_trips",
  keys = ["VendorID"],
  sequence_by = "sequence_struct",
  stored_as_scd_type = 2,
  track_history_column_list = ["hashed_name"]
)
