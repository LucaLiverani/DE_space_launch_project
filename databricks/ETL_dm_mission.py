# Databricks notebook source
# MAGIC %run /Users/l.liverani@reply.it/common_function

# COMMAND ----------

import pandas as pd
import json

staging_launch_df = spark.sql("select mission from default.staging_launch")
display(staging_launch_df)

# COMMAND ----------

columns = ["mission.id", "mission.name", "mission.description", "mission.type", "mission.orbit"]

mission_df = staging_launch_df[columns].dropDuplicates()
display(mission_df)

#spark.sql("drop table default.dm_mission")
#mission_df.write.saveAsTable("default.dm_mission")

# COMMAND ----------

table_name = "dm_mission"

mission_df.createOrReplaceTempView(f"{table_name}_update")

target_table = f"default.{table_name}"
upd_table = f"{table_name}_update"
merge_key = "id"

merge(target_table, upd_table, merge_key)
