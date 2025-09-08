from pyspark.sql import functions as F
from delta.tables import DeltaTable as D
from datetime import datetime
import sys

def get_daily(suffix: str) -> str:
    """
    Generates the daily file name for given suffix.

    ### Args
    - suffix (str): the category name

    ### Return
    - Returns the daily name based on todays date and suffix.
    """
    now = datetime.today().strftime('%d-%m-%Y')
    return f"{now}-daily-{suffix}.json"

def merge_db(df, table_name: str):
    """
    Updates matches on id and inserts non-matches.

    ### Args
    - df (pyspark.DataFrame): The processed table to be merged into the database
    - table_name (str): The name of the table to merge into

    ### Return
    - None
    """
    table = D.forName(spark, f"webscraper.silver.{table_name}")
    
    table.alias("l").merge(
        df.alias("r"),"l.id = r.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

def process_cell(df):
    """
    This function enforces type casting, converts select columns to structs,
    renames the columns and drops duplicates for cell table.

    ### Args
    - df (pyspark.DataFrame): the listing table.

    ### Return
    - Returns a processed structured table with enforced type casting.
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

def process_moto(df):
    """
    This function enforces type casting, converts select columns to structs,
    renames the columns and drops duplicates for motorcycle table.

    ### Args
    - df (pyspark DataFrame): the listing table

    ### Return
    - Returns a processed structured table with enforced type casting
    """
    curr_timestamp = F.current_timestamp()
    return (
        df
        .withColumn("id", F.col("ID").cast("long"))
        .withColumn("category", F.lit("Motorcycle"))
        .withColumn("OdometerUnits", F.regexp_replace(
            "OdometerUnits", "KMT", "KM"))
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
        .withColumn("model", F.struct(
            F.col("Brand").cast("string").alias("brand"),
            F.col("Model").cast("string").alias("name"),
            F.col("Year").cast("int").alias("year")))
        .withColumn("odometer", F.struct(
            F.col("OdometerUnits").cast("string").alias("unit"),
            F.col("Odometer").cast("double").alias("value")))
        .withColumn("date", F.col("Date").cast("date"))
        .withColumn("processed_at", curr_timestamp)
        .dropna(subset=["id"])
        .dropDuplicates(["id"])
        .select("id", "category", "info", "location", "cost",
                "model", "odometer", "date", "processed_at")
    )

def update(file_dir: str, suffix: str, table: str, preprocess: function, on_all: bool=False):
    """
    This function reads data, processes it, then merges it to the database.

    ### Args
    - file_dir (str): location of where the raw data is stored
    - suffix (str): the suffix of the daily name
    - table (str): the name of the table to be updated
    - preporcess (function): a function for processing the table
    - on_all (bool): run operation on all or single raw data source.

    ### Return
    - None
    """
    # select the files to process
    if on_all:
        df = spark.read.option("multiline", "true").json(file_dir + "*.json")
    else:
        df = spark.read.option("multiline", "true").json(file_dir + get_daily(suffix))
    
    # process data
    df = preprocess(df)
    
    # try to merge (spark lazy; exceptions will occur here)
    try:
        merge_db(df, table)
    except:
        print(f"Failed to update the {table}")
        return

    print(f"Successfully updated the {table}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise ValueError("Error: must provide category and bool.")
    
    all_raw = sys.argv[1].lower().strip()
    category = sys.argv[2].lower().strip()

    if all_raw == "false":
        all_raw = False
    else:
        all_raw = True

    if category == "cell":
        LISTING_LOC = "/Volumes/webscraper/bronze/cellphone_raw/Kijiji/"
        suffix = "cell"
        table_name = "cell_listings"
        proc_fn = process_cell
    else:
        LISTING_LOC = "/Volumes/webscraper/bronze/motorcycle_raw/Kijiji/"
        suffix = "motorcycle"
        table_name = "motorcycle_listings"
        proc_fn = process_moto
    
    update(LISTING_LOC, suffix, table_name, proc_fn, all_raw)
