# Databricks notebook source
catalog = "`data-engineering`"

from pyspark.sql.functions import col, when, count, avg, expr

df_silver_salaries_clean = df_silver_salaries.filter(col("salary_quality_flag").isNull())


postings_with_salary = df_silver_postings.join(df_silver_salaries_clean, "job_id", "inner").select("job_id").distinct().count()
total_postings = df_silver_postings.count()
print(f"Postings with clean salary data: {postings_with_salary:,} / {total_postings:,} "
      f"({100 * postings_with_salary / total_postings:.1f}%)")

# --- 1. gold_skills_by_seniority ---
df_skills_seniority = (
    df_silver_postings
    .select("job_id", "formatted_experience_level")
    .join(df_silver_job_skills.select("job_id", "skill_name"), "job_id", "inner")
    .groupBy("formatted_experience_level", "skill_name")
    .agg(count("*").alias("posting_count"))
    .orderBy("formatted_experience_level", col("posting_count").desc())
)
df_skills_seniority.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold_layer.gold_skills_by_seniority")
print("gold_skills_by_seniority rows:", spark.table(f"{catalog}.gold_layer.gold_skills_by_seniority").count())

# --- 2. gold_skills_by_salary_band ---
df_with_band = (
    df_silver_salaries_clean
    .select("job_id", "annual_min_salary")
    .withColumn(
        "salary_band",
        when(col("annual_min_salary") < 60000, "<60k")
        .when(col("annual_min_salary") < 100000, "60-100k")
        .when(col("annual_min_salary") < 150000, "100-150k")
        .otherwise("150k+")
    )
)
df_skills_salary_band = (
    df_with_band
    .join(df_silver_job_skills.select("job_id", "skill_name"), "job_id", "inner")
    .groupBy("salary_band", "skill_name")
    .agg(count("*").alias("posting_count"))
    .orderBy("salary_band", col("posting_count").desc())
)
df_skills_salary_band.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold_layer.gold_skills_by_salary_band")
print("gold_skills_by_salary_band rows:", spark.table(f"{catalog}.gold_layer.gold_skills_by_salary_band").count())

# --- 3. gold_seniority_salary_summary ---
df_seniority_salary_summary = (
    df_silver_postings
    .select("job_id", "formatted_experience_level")
    .join(df_silver_salaries_clean.select("job_id", "annual_min_salary", "annual_max_salary"), "job_id", "inner")
    .groupBy("formatted_experience_level")
    .agg(
        count("*").alias("posting_count"),
        avg("annual_min_salary").alias("avg_annual_min_salary"),
        avg("annual_max_salary").alias("avg_annual_max_salary")
    )
    .orderBy("avg_annual_min_salary")
)
df_seniority_salary_summary.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold_layer.gold_seniority_salary_summary")
df_seniority_salary_summary.show(truncate=False)