# Databricks notebook source
#migration of bronze_postings to silver_layer

from pyspark.sql.functions import col, from_unixtime

catalog = "`data-engineering`"

df_bronze_postings = spark.table(f"{catalog}.bronze_layer.bronze_postings")

df_silver_postings = (
    df_bronze_postings
    .filter(col("job_id").isNotNull())
    .withColumn("listed_time_ts", from_unixtime(col("listed_time").cast("double")/1000).cast("timestamp"))
    .withColumn("original_listed_time_ts", from_unixtime(col("original_listed_time").cast("double")/1000).cast("timestamp"))
    .withColumn("expiry_ts", from_unixtime(col("expiry").cast("double")/1000).cast("timestamp"))
    .withColumn("closed_time_ts", from_unixtime(col("closed_time").cast("double")/1000).cast("timestamp"))
    .dropDuplicates(["job_id"])
)

df_silver_postings.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver_layer.silver_postings")

#verify
print("job_id nulls in bronze:", df_bronze_postings.filter(col("job_id").isNull()).count())
print("silver_postings rows:", spark.table(f"{catalog}.silver_layer.silver_postings").count())
print("distinct job_id count:", df_silver_postings.select("job_id").distinct().count())
              

              

# COMMAND ----------

###migration bronze_job_skills to silver_layer

catalog = "`data-engineering`"

df_bronze_job_skills = spark.table(f"{catalog}.bronze_layer.bronze_job_skills")
df_mappings = spark.read.csv(
    "/Volumes/data-engineering/default/databricks-data-platform/data/mappings/skills.csv", header=True,
)

display(df_mappings.head(5))
display(df_bronze_job_skills.head(5))


df_silver_job_skills = (
    df_bronze_job_skills
    .dropDuplicates(["job_id", "skill_abr"])
    .join(df_mappings, on="skill_abr", how="left")
)   

df_silver_job_skills.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver_layer.silver_job_skills")

# Verify
print("silver_job_skills rows:", spark.table(f"{catalog}.silver_layer.silver_job_skills").count())
print("unmatched skill_abr (null skill_name after join):",
      df_silver_job_skills.filter(col("skill_name").isNull()).count())

# COMMAND ----------

# DBTITLE 1,from pyspark.sql.functions import when
df_bronze_salaries = spark.table(f"{catalog}.bronze_layer.bronze_salaries")

from pyspark.sql.functions import coalesce, col, when


def annualize(amount_col):
    return (
        when(col("pay_period") == "YEARLY", amount_col)
        .when(col("pay_period") == "MONTHLY", amount_col * 12)
        .when(col("pay_period") == "HOURLY", amount_col * 2080)
        .when(col("pay_period") == "WEEKLY", amount_col * 52)
        .when(col("pay_period") == "BIWEEKLY", amount_col * 26)
        .otherwise(None)
    )

df_silver_salaries = (
    df_bronze_salaries
    .dropDuplicates(["salary_id"])
    .withColumn("annual_min_salary", annualize(coalesce(col("min_salary"), col("med_salary"))))
    .withColumn("annual_max_salary", annualize(coalesce(col("max_salary"), col("med_salary"))))
    .withColumn(
        "salary_quality_flag",
        # No real hourly-wage posting exceeds ~$300/hr; anything above that under
        # pay_period=HOURLY is an annual figure mislabeled as hourly in the source.
        when((col("pay_period") == "HOURLY") & (col("min_salary") > 300), "implausible_hourly_rate")
        .otherwise(None)
    )
)

df_silver_salaries.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.silver_layer.silver_salaries")

print("Flagged rows:", df_silver_salaries.filter(col("salary_quality_flag").isNotNull()).count())
print("Total rows:", df_silver_salaries.count())