from pyspark.sql import functions as F
from delta.tables import DeltaTable as D

def normalize_whitespace(col):
    """
    This function standardizes the whitespaces between characters
    and strips off whitespaces on either start or end of string.

    Args:
    ---
    - col (Column): column of a pyspark dataframe that contains string to be processed

    Return:
    ---
    - Returns the string with whitespaces normalized.
    """
    return F.trim(F.regexp_replace(col, r"\s+", " "))

def preprocess(df):
    """
    This function normalizes whitespaces, then removes empty or null values
    then drops duplicates

    Args:
    ---
    df (pyspark DataFrame): the table with brand and model to process

    Return:
    ---
    - Returns a processed table with normalized whitespaces, no nulls and
    duplicates removed.
    """
    return (
        df
        .withColumn("brand", normalize_whitespace(F.col("brand")))
        .withColumn("model", normalize_whitespace(F.col("model")))
        .withColumn("length", F.col("length").cast("int"))
        .dropna(subset=["brand", "model", "pattern", "length"])
        .filter((F.col("brand") != "") & (F.col("model") != "") & (F.col("pattern") != ""))
        .dropDuplicates(["brand", "model"])
        .dropDuplicates(["brand", "pattern"])
    )

def merge_to_db(df):
    """
    Merges df into table, updating url for matches and inserting if there
    are no matches.

    Args:
    ---
    - df (pyspark DataFrame): dataframe to merge, will not pre-process, only merge.

    Return:
    ---
    - None
    """
    table = D.forName(spark, "webscraper.silver.cell_models")
    
    table.alias("l").merge(
        df.select("brand", "model", "pattern", "length", "url").alias("r"),
        "l.brand = r.brand AND l.model = r.model"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

if __name__ == '__main__':
    GSMARENA_LOC = "/Volumes/webscraper/bronze/cellphone_raw/GSMArena/"
    model_df = spark.read.option("header", True).csv(GSMARENA_LOC + "cell_models.csv")
    model_df = preprocess(model_df)
    merge_to_db(model_df)
    print("Updating cell_model table completed!")
