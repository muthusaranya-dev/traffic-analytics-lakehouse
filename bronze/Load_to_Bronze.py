# Databricks notebook source
# MAGIC %md
# MAGIC # Road Data Loading

# COMMAND ----------

roads_df = spark.table("default.raw_roads_1")

roads_bronze = (
    roads_df
    .withColumnRenamed("Road ID", "road_id")
    .withColumnRenamed("Road category id", "road_category_id")
    .withColumnRenamed("Road category", "road_category")
    .withColumnRenamed("Region id", "region_id")
    .withColumnRenamed("Region name", "region_name")
    .withColumnRenamed("Total link length km", "total_link_length_km")
    .withColumnRenamed("Total link length miles", "total_link_length_miles")
    .withColumnRenamed("All motor vehicles", "all_motor_vehicles")
)

roads_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.roads_raw")

# COMMAND ----------

spark.sql("SELECT * FROM bronze.roads_raw LIMIT 10").show()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Describe Table

# COMMAND ----------

spark.table("bronze.roads_raw").describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Tables in Bronze

# COMMAND ----------

spark.sql("SHOW TABLES IN bronze").show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Traffic Data Loading

# COMMAND ----------

traffic_df = spark.table("default.raw_traffic_1")

traffic_bronze = (
    traffic_df
    .withColumnRenamed("Record ID", "record_id")
    .withColumnRenamed("Count point id", "count_point_id")
    .withColumnRenamed("Direction of travel", "direction_of_travel")
    .withColumnRenamed("Year", "year")
    .withColumnRenamed("Count date", "count_date")
    .withColumnRenamed("hour", "hour")
    .withColumnRenamed("Region id", "region_id")
    .withColumnRenamed("Region name", "region_name")
    .withColumnRenamed("Local authority name", "local_authority_name")
    .withColumnRenamed("Road name", "road_name")
    .withColumnRenamed("Road Category ID", "road_category_id")
    .withColumnRenamed("Start junction road name", "start_junction_road_name")
    .withColumnRenamed("End junction road name", "end_junction_road_name")
    .withColumnRenamed("Latitude", "latitude")
    .withColumnRenamed("Longitude", "longitude")
    .withColumnRenamed("Link length km", "link_length_km")
    .withColumnRenamed("Pedal cycles", "pedal_cycles")
    .withColumnRenamed("Two wheeled motor vehicles", "two_wheeled_motor_vehicles")
    .withColumnRenamed("Cars and taxis", "cars_and_taxis")
    .withColumnRenamed("Buses and coaches", "buses_and_coaches")
    .withColumnRenamed("LGV Type", "lgv_type")
    .withColumnRenamed("HGV Type", "hgv_type")
    .withColumnRenamed("EV Car", "ev_car")
    .withColumnRenamed("EV Bike", "ev_bike")
)

traffic_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.traffic_raw")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Describe Tables

# COMMAND ----------

spark.table("bronze.traffic_raw").describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Show tables in bronze

# COMMAND ----------


spark.sql("SHOW TABLES IN bronze").show()