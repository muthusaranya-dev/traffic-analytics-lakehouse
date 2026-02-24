# Databricks notebook source
# MAGIC %md
# MAGIC # Create Bronze Roads Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.roads_raw (
# MAGIC   road_id INT,
# MAGIC   road_category_id INT,
# MAGIC   road_category STRING,
# MAGIC   region_id INT,
# MAGIC   region_name STRING,
# MAGIC   total_link_length_km DOUBLE,
# MAGIC   total_link_length_miles DOUBLE,
# MAGIC   all_motor_vehicles DOUBLE
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Bronze Traffic Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.traffic_raw (
# MAGIC   record_id BIGINT,
# MAGIC   count_point_id BIGINT,
# MAGIC   direction_of_travel STRING,
# MAGIC   year INT,
# MAGIC   count_date STRING,
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

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bronze;
# MAGIC DESCRIBE TABLE bronze.roads_raw;
# MAGIC DESCRIBE TABLE bronze.traffic_raw;