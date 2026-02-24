# Databricks notebook source
# MAGIC %md
# MAGIC # Create Schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;