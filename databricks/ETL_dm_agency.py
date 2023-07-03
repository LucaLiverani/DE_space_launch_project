# Databricks notebook source
# MAGIC %run /Users/l.liverani@reply.it/common_function

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from default.staging_launch

# COMMAND ----------

import pandas as pd
import json

staging_launch_df = spark.sql("select launch_service_provider from default.staging_launch")#.toPandas()

display(staging_launch_df)

# COMMAND ----------

columns = ['launch_service_provider.id', 'launch_service_provider.name', 'launch_service_provider.administrator', 'launch_service_provider.country_code', 'launch_service_provider.description', 'launch_service_provider.founding_year', 'launch_service_provider.info_url', 'launch_service_provider.launchers', 'launch_service_provider.type']

agency_df = staging_launch_df[columns].dropDuplicates()
display(agency_df)

#spark.sql("drop table default.dm_agency")
#agency_df.write.saveAsTable("default.dm_agency")

# COMMAND ----------

table_name = "dm_agency"

agency_df.createOrReplaceTempView(f"{table_name}_update")

target_table = f"default.{table_name}"
upd_table = f"{table_name}_update"
merge_key = "id"

merge(target_table, upd_table, merge_key)
