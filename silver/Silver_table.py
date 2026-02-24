# Databricks notebook source
# MAGIC %md
# MAGIC # 1.Create Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.roads_clean (
# MAGIC   road_id INT,
# MAGIC   road_category_id INT,
# MAGIC   road_category STRING,
# MAGIC   region_id INT,
# MAGIC   region_name STRING,
# MAGIC   total_link_length_km DOUBLE,
# MAGIC   total_link_length_miles DOUBLE,
# MAGIC   all_motor_vehicles DOUBLE
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Silver Road Transform

# COMMAND ----------

from pyspark.sql.functions import col, trim

roads = spark.table("bronze.roads_raw")

silver_roads = roads.select(
    col("road_id").cast("int").alias("road_id"),
    col("road_category_id").cast("int").alias("road_category_id"),
    col("road_category").cast("string").alias("road_category"),
    col("region_id").cast("int").alias("region_id"),
    trim(col("region_name")).alias("region_name"),
    col("total_link_length_km").cast("double").alias("total_link_length_km"),
    col("total_link_length_miles").cast("double").alias("total_link_length_miles"),
    col("all_motor_vehicles").cast("double").alias("all_motor_vehicles")
)

silver_roads.write.format("delta").mode("overwrite").saveAsTable("silver.roads_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver.roads_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.Create Silver Traffic Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.traffic_clean (
# MAGIC   record_id BIGINT,
# MAGIC   count_point_id BIGINT,
# MAGIC   direction_of_travel STRING,
# MAGIC   year INT,
# MAGIC   count_ts TIMESTAMP,
# MAGIC   count_date DATE,
# MAGIC   hour INT,
# MAGIC   region_id INT,
# MAGIC   region_name STRING,
# MAGIC   local_authority_name STRING,
# MAGIC   road_name STRING,
# MAGIC   road_category_id INT,
# MAGIC   start_junction_road_name STRING,
# MAGIC   end_junction_road_name STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   link_length_km DOUBLE,
# MAGIC   pedal_cycles BIGINT,
# MAGIC   two_wheeled_motor_vehicles BIGINT,
# MAGIC   cars_and_taxis BIGINT,
# MAGIC   buses_and_coaches BIGINT,
# MAGIC   lgv_type BIGINT,
# MAGIC   hgv_type BIGINT,
# MAGIC   ev_car BIGINT,
# MAGIC   ev_bike BIGINT
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##  2.1 Silver Traffic Transform

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, to_date

traffic = spark.table("bronze.traffic_raw")

silver_traffic = traffic.select(
    col("record_id").cast("long").alias("record_id"),
    col("count_point_id").cast("long").alias("count_point_id"),
    trim(col("direction_of_travel")).alias("direction_of_travel"),
    col("year").cast("int").alias("year"),
    to_timestamp(col("count_date"), "M/d/yyyy H:mm").alias("count_ts"),
    to_date(to_timestamp(col("count_date"), "M/d/yyyy H:mm")).alias("count_date"),
    col("hour").cast("int").alias("hour"),
    col("region_id").cast("int").alias("region_id"),
    trim(col("region_name")).alias("region_name"),
    trim(col("local_authority_name")).alias("local_authority_name"),
    trim(col("road_name")).alias("road_name"),
    col("road_category_id").cast("int").alias("road_category_id"),
    trim(col("start_junction_road_name")).alias("start_junction_road_name"),
    trim(col("end_junction_road_name")).alias("end_junction_road_name"),
    col("latitude").cast("double").alias("latitude"),
    col("longitude").cast("double").alias("longitude"),
    col("link_length_km").cast("double").alias("link_length_km"),
    col("pedal_cycles").cast("long").alias("pedal_cycles"),
    col("two_wheeled_motor_vehicles").cast("long").alias("two_wheeled_motor_vehicles"),
    col("cars_and_taxis").cast("long").alias("cars_and_taxis"),
    col("buses_and_coaches").cast("long").alias("buses_and_coaches"),
    col("lgv_type").cast("long").alias("lgv_type"),
    col("hgv_type").cast("long").alias("hgv_type"),
    col("ev_car").cast("long").alias("ev_car"),
    col("ev_bike").cast("long").alias("ev_bike")
)

silver_traffic.write.format("delta").mode("overwrite").saveAsTable("silver.traffic_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver.traffic_clean;