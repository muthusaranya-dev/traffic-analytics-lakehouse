# Databricks notebook source
# MAGIC %md
# MAGIC # 1.Create Dimention Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Dim Region

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_region
# MAGIC (
# MAGIC   dim_region_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   region_id INT,
# MAGIC   region_name STRING,
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Dim Road

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_road
# MAGIC (
# MAGIC   dim_road_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   road_name STRING,
# MAGIC   road_category_id INT,
# MAGIC   region_id INT,
# MAGIC
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   is_current BOOLEAN,
# MAGIC
# MAGIC   CONSTRAINT pk_dim_road PRIMARY KEY (dim_road_key)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.Dim Date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_date
# MAGIC (
# MAGIC   dim_date_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   count_date DATE,                             
# MAGIC   year INT,
# MAGIC   month INT,
# MAGIC   day INT,
# MAGIC
# MAGIC   CONSTRAINT pk_dim_date 
# MAGIC   PRIMARY KEY (dim_date_key)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.Dim Count_point

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_count_point
# MAGIC (
# MAGIC   dim_count_point_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   count_point_id INT,                          
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   local_authority_name STRING,
# MAGIC
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   is_current BOOLEAN,
# MAGIC
# MAGIC   CONSTRAINT pk_dim_count_point 
# MAGIC   PRIMARY KEY (dim_count_point_key)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.Create Fact Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_traffic
# MAGIC (
# MAGIC   dim_count_point_key BIGINT,    
# MAGIC   dim_date_key BIGINT,            
# MAGIC
# MAGIC   hour INT,
# MAGIC   pedal_cycles INT,
# MAGIC   two_wheeled_motor_vehicles INT,
# MAGIC   cars_and_taxis INT,
# MAGIC   buses_and_coaches INT,
# MAGIC   lgv_type INT,
# MAGIC   hgv_type INT,
# MAGIC   ev_car INT,
# MAGIC   ev_bike INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gold;
# MAGIC
# MAGIC SELECT COUNT(*) FROM gold.fact_traffic;
# MAGIC
# MAGIC SELECT * FROM gold.fact_traffic LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_region' AS table_name, COUNT(*) AS row_count FROM gold.dim_region
# MAGIC UNION ALL
# MAGIC SELECT 'dim_road', COUNT(*) FROM gold.dim_road
# MAGIC UNION ALL
# MAGIC SELECT 'dim_date', COUNT(*) FROM gold.dim_date
# MAGIC UNION ALL
# MAGIC SELECT 'dim_count_point', COUNT(*) FROM gold.dim_count_point
# MAGIC UNION ALL
# MAGIC SELECT 'fact_traffic', COUNT(*) FROM gold.fact_traffic;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count_point_id, count_date, hour, COUNT(*) cnt
# MAGIC FROM gold.fact_traffic
# MAGIC GROUP BY count_point_id, count_date, hour
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.Create the Star Schema View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_traffic_star AS
# MAGIC SELECT
# MAGIC   f.count_point_id,
# MAGIC   cp.latitude,
# MAGIC   cp.longitude,
# MAGIC   cp.local_authority_name,
# MAGIC
# MAGIC   d.count_date,
# MAGIC   d.year,
# MAGIC   d.month,
# MAGIC   d.day,
# MAGIC
# MAGIC   f.hour,
# MAGIC
# MAGIC   f.pedal_cycles,
# MAGIC   f.two_wheeled_motor_vehicles,
# MAGIC   f.cars_and_taxis,
# MAGIC   f.buses_and_coaches,
# MAGIC   f.lgv_type,
# MAGIC   f.hgv_type,
# MAGIC   f.ev_car,
# MAGIC   f.ev_bike
# MAGIC FROM gold.fact_traffic f
# MAGIC LEFT JOIN gold.dim_count_point cp
# MAGIC   ON f.count_point_id = cp.count_point_id
# MAGIC LEFT JOIN gold.dim_date d
# MAGIC   ON f.count_date = d.count_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Monthly trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.year, d.month, SUM(f.cars_and_taxis) AS cars
# MAGIC FROM gold.fact_traffic f
# MAGIC JOIN gold.dim_date d ON f.count_date = d.count_date
# MAGIC GROUP BY d.year, d.month
# MAGIC ORDER BY d.year, d.month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Hourly traffic pattern

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT hour, SUM(cars_and_taxis) AS cars
# MAGIC FROM gold.fact_traffic
# MAGIC GROUP BY hour
# MAGIC ORDER BY hour;

# COMMAND ----------

fact_pdf = spark.table("gold.fact_traffic").toPandas()
display(fact_pdf)

# COMMAND ----------

cp_pdf = spark.table("gold.dim_count_point").toPandas()
display(cp_pdf)

# COMMAND ----------

date_pdf = spark.table("gold.dim_date").toPandas()
display(date_pdf)