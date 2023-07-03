# Databricks notebook source
# MAGIC %run /Users/l.liverani@reply.it/common_function

# COMMAND ----------

import pandas as pd
import json

staging_launch_df = spark.sql("select pad from default.staging_launch")
display(staging_launch_df)

# COMMAND ----------

columns = ['pad.id', 'pad.name', 'pad.country_code', 'pad.latitude', 'pad.longitude', 'pad.orbital_launch_attempt_count', 'pad.total_launch_count', 'pad.location', 'pad.map_url']

pad_df = staging_launch_df[columns].dropDuplicates()
display(pad_df)

#spark.sql("drop table if exists default.dm_pad")
#pad_df.write.saveAsTable("default.dm_pad")

# COMMAND ----------

table_name = "dm_pad"

pad_df.createOrReplaceTempView(f"{table_name}_update")

target_table = f"default.{table_name}"
upd_table = f"{table_name}_update"
merge_key = "id"

merge(target_table, upd_table, merge_key)
