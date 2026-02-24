# Databricks notebook source
# MAGIC %md
# MAGIC # Load Hub/Link/Sat

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

t = spark.table("silver.traffic_clean")

# COMMAND ----------

hub_region = (
    t.select(col("region_id").cast("int").alias("region_id"))
     .dropDuplicates()
     .withColumn("load_ts", current_timestamp())
     .select("region_id", "load_ts")
)

hub_region.write.format("delta").mode("overwrite").saveAsTable("silver.hub_region")