from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from dotenv import load_dotenv
import os
import sys

# Spark on Windows setup !!

hadoop_home = "C:/hadoop"
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + os.pathsep + os.environ['PATH']
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder \
    .appName("XTD_Labs_Historical_Data_Processing") \
    .master("local") \
    .config("spark.jars", "file:///C:/drivers/postgresql-42.7.11.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# PostgreSQL Config
from dotenv import load_dotenv
load_dotenv()

PG_URL    = f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
PG_USER   = os.getenv('PG_USER')
PG_PASS   = os.getenv('PG_PASSWORD')
PG_DRIVER = "org.postgresql.Driver"

def write_to_pg(df, table, mode="append"):
    df.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", table) \
        .option("user", PG_USER) \
        .option("password", PG_PASS) \
        .option("driver", PG_DRIVER) \
        .mode("append") \
        .save()
    print(f"  ✔ Loaded to {table}")

# 1.BRONZE : Read All Bronce data (All raw JSON files at once)
bronze_path = os.path.join(BASE_DIR, "data", "bronzes", "*.json").replace("\\", "/")
raw_df = spark.read.json(bronze_path, multiLine=True)
# Count the total number of 30-minute intervals (rows) in the raw data for auditing purposes 
data_count = raw_df.count()
print(f"Audit: Total 30-minute intervals found in Bronze: {data_count}")

# 2. SILVER LAYER i: Flattening the data points
exploded_df = raw_df.select(
    F.col("from").alias("timestamp"),
    F.explode(F.col("regions")).alias("region")
)

silver_df = exploded_df.select(
    "timestamp",
    F.col("region.regionid").alias("regionid"),
    F.col("region.shortname").alias("shortname"),
    F.col("region.dnoregion").alias("dno"),
    F.col("region.intensity.forecast").alias("intensity"),
    F.col("region.intensity.index").alias("index"),
    F.explode(F.col("region.generationmix")).alias("mix")
)

# 3. SILVER LAYER ii: 
silver_pivoted = silver_df.groupBy("regionid", "shortname", "dno", "timestamp", "intensity", "index") \
    .pivot("mix.fuel") \
    .agg(F.first("mix.perc"))

# 4. GOLD LAYER: Aggregate to Daily Averages
print('Starting Gold Layer Aggregation')
gold_df = silver_pivoted.withColumn("date_recorded", F.to_date("timestamp")) \
    .groupBy("regionid", "date_recorded") \
    .agg(
        F.first("shortname").alias("shortname"),
        F.first("dno").alias("dno"),
        F.round(F.mean("intensity"), 2).alias("intensity_avg"),
        F.mode("index").alias("index_mode"),
        F.round(F.mean("biomass"), 2).alias("fuel_biomass"),
        F.round(F.mean("coal"), 2).alias("fuel_coal"),
        F.round(F.mean("gas"), 2).alias("fuel_gas"),
        F.round(F.mean("hydro"), 2).alias("fuel_hydro"),
        F.round(F.mean("imports"), 2).alias("fuel_imports"),
        F.round(F.mean("nuclear"), 2).alias("fuel_nuclear"),
        F.round(F.mean("other"), 2).alias("fuel_other"),
        F.round(F.mean("solar"), 2).alias("fuel_solar"),
        F.round(F.mean("wind"), 2).alias("fuel_wind")
    ).orderBy("date_recorded", "regionid")

# Final audit count of records in Gold layer (should be 14 regions * number of days)
final_count = gold_df.count()
print(f"Audit: Final processed research records in Gold: {final_count}")

# 5. SAVE
gold_df.write.csv("data/gold_carbon_historical.csv", header=True, mode="overwrite")
gold_df.show(5)



# BUILD STAR SCHEMA DATAFRAMES

# dim_region: one row per region
dim_region = gold_df.select("regionid", "shortname", "dno") \
    .dropDuplicates(["regionid"]).orderBy("regionid")

# dim_date: one row per date with calendar attributes
dim_date = gold_df.select("date_recorded").dropDuplicates() \
    .withColumn("year",        F.year("date_recorded")) \
    .withColumn("quarter",     F.quarter("date_recorded")) \
    .withColumn("month",       F.month("date_recorded")) \
    .withColumn("month_name",  F.date_format("date_recorded", "MMMM")) \
    .withColumn("week",        F.weekofyear("date_recorded")) \
    .withColumn("day_of_week", F.date_format("date_recorded", "EEEE")) \
    .withColumn("is_weekend",  F.dayofweek("date_recorded").isin([1, 7]).cast("boolean")) \
    .orderBy("date_recorded")

# dim_index: carbon intensity category labels
dim_index = gold_df.select("index_mode").dropDuplicates() \
    .withColumnRenamed("index_mode", "index_label") \
    .withColumn("index_id", F.monotonically_increasing_id().cast(IntegerType())) \
    .select("index_id", "index_label")

# fact table: measurements with foreign keys
fact_df = gold_df \
    .join(dim_index.withColumnRenamed("index_label", "index_mode"), on="index_mode", how="left") \
    .select("regionid", "date_recorded", "index_id", "intensity_avg",
            "fuel_biomass", "fuel_coal", "fuel_gas", "fuel_hydro",
            "fuel_imports", "fuel_nuclear", "fuel_other", "fuel_solar", "fuel_wind") \
    .orderBy("date_recorded", "regionid")


#TRUNCATE TABLES BEFORE LOADING
import psycopg2
print("Truncating existing tables...")
conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    database=os.getenv("PG_DATABASE"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("TRUNCATE TABLE carbon.fact_historical_averages RESTART IDENTITY CASCADE;")
cur.execute("TRUNCATE TABLE carbon.dim_region RESTART IDENTITY CASCADE;")
cur.execute("TRUNCATE TABLE carbon.dim_date RESTART IDENTITY CASCADE;")
cur.execute("TRUNCATE TABLE carbon.dim_index RESTART IDENTITY CASCADE;")
print("  ✔ Tables truncated")
conn.close()

# 6. LOAD TO POSTGRESQL ─────────────────────────────────────────
# Dimensions must load BEFORE the fact table (foreign key constraint)
print("\nLoading to PostgreSQL...")
write_to_pg(dim_region, "carbon.dim_region", mode="append")  # Overwrite dimensions to ensure clean state
write_to_pg(dim_date,   "carbon.dim_date", mode="append")    # Overwrite dimensions to ensure clean state
write_to_pg(dim_index,  "carbon.dim_index", mode="append")
write_to_pg(fact_df,    "carbon.fact_historical_averages", mode="append")  # Overwrite facts for fresh data load

print(f"\nPipeline complete! {final_count:,} rows loaded to xtd_warehouse.")
gold_df.show(5)

# For Postgres Connection and loading
# gold_df.write.format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/xtd_warehouse") \
#     .option("dbtable", "carbon.fact_historical_averages") \
#     .option("user", "postgres") \
#     .option("password", "password") \
#     .save()
