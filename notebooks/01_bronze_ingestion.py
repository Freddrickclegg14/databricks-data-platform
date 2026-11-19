# Databricks notebook source
# DBTITLE 1,Load postings.csv
catalog = "data-engineering"
volume_path = f"/Volumes/{catalog}/default/databricks-data-platform/data/"

df_postings = (
    spark.read
    .option("header", True)
    .option("multiLine", "true")
    .option("escape", '"')
    .csv(f"{volume_path}/postings.csv")
)
df_postings.write.format("delta").mode("overwrite").saveAsTable(f"`{catalog}`.bronze_layer.bronze_postings")

df_jobs_skills = spark.read.csv(f"{volume_path}jobs/job_skills.csv", header=True, inferSchema=True)
df_jobs_skills.write.format("delta").mode("overwrite").saveAsTable(f"`{catalog}`.bronze_layer.bronze_job_skills")

df_salaries = spark.read.csv(f"{volume_path}jobs/salaries.csv", header=True, inferSchema=True)
df_salaries.write.format("delta").mode("overwrite").saveAsTable(f"`{catalog}`.bronze_layer.bronze_salaries")

# COMMAND ----------

#data_verification

for table, expected in [
    ("bronze_postings", 123_849),
    ("bronze_job_skills", 213_768),
    ("bronze_salaries", 40_785),
]:
    actual = spark.read.table(f"`{catalog}`.bronze_layer.{table}").count()
    match = "MATCH" if actual == expected else "MISMATCH"
    print(f"{table}: {actual:,} rows (expected {expected:,}) {match}")