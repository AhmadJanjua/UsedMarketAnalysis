from pyspark.sql import functions as F
from delta.tables import DeltaTable as D
from datetime import datetime
import sys

def get_daily_cell():
    """
    Returns the daily cellphone file.

    Args:
    ---
    - None

    Return:
    - Returns cellphone listing name as a string
    """
    now = datetime.today().strftime('%d-%m-%Y')
    return f"{now}-daily-cell.json"

def preprocess(df):
    """
    This function enforces type casting, converts select columns to structs,
    renames the columns and drops duplicates

    Args:
    ---
    - df (pyspark DataFrame): the listing table

    Return:
    ---
    - Returns a processed structured table with enforced type casting
    """
    curr_timestamp = F.current_timestamp()
    return (
        df
        .withColumn("id", F.col("ID").cast("long"))
        .withColumn("category", F.lit("Product"))
        .withColumn("info", F.struct(
            F.col("Title").cast("string").alias("title"),
            F.col("Description").cast("string").alias("description"),
            F.col("url").cast("string").alias("url")))
        .withColumn("location", F.struct(
            F.col("Country").cast("string").alias("country"),
            F.col("City").cast("string").alias("city"),
            F.col("Address").cast("string").alias("address"),
            F.struct(
                F.col("Latitude").cast("double").alias("lat"),
                F.col("Longitude").cast("double").alias("lon")
            ).alias("coordinate")))
        .withColumn("cost", F.struct(
            F.col("Currency").cast("string").alias("currency"),
            F.col("Price").cast("double").alias("price")))
        .withColumn("date", F.col("Date").cast("date"))
        .withColumn("processed_at", curr_timestamp)
        .dropna(subset=["id"])
        .dropDuplicates(["id"])
        .select("id", "category", "info", "location",
                "cost", "date", "processed_at")
    )

def merge_to_db(df):
    """
    Merges df into table, updates old listings and adds new ones if they exist

    Args:
    ---
    - df (pyspark DataFrame): dataframe to merge, will not pre-process, only merge.

    Return:
    ---
    - None
    """
    table = D.forName(spark, "webscraper.silver.cell_listings")
    
    table.alias("l").merge(
        df.alias("r"),"l.id = r.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

def run_update(all: bool=False):
    LISTING_LOC = "/Volumes/webscraper/bronze/cellphone_raw/Kijiji/"
    if all:
        cell_df = spark.read.option("multiline", "true").json(LISTING_LOC + "*.json")
    else:
        cell_df = spark.read.option("multiline", "true").json(LISTING_LOC + get_daily_cell())
    
    tmp = preprocess(cell_df)
    
    try:
        merge_to_db(tmp)
    except:
        print("Failed to update the cell_listings")
        return

    print("Successfully updated the cell_listings")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError("Error: failed to retrieve all arguments.")

    run_all = ("True" == sys.argv[1])
    run_update(run_all)
