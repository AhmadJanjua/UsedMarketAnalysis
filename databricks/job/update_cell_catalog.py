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
        .dropna(subset=["brand", "model"])
        .filter((F.col("brand") != "") & (F.col("model") != ""))
        .dropDuplicates(["brand", "model"])
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
    table = D.forName(spark, "webscraper.silver.cell_catalog")
    
    table.alias("l").merge(
        df.select("brand", "model", "url").alias("r"),
        "l.brand = r.brand AND l.model = r.model"
    )\
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

if __name__ == '__main__':
    GSMARENA_LOC = "/Volumes/webscraper/bronze/cellphone_raw/GSMArena/"
    catalog_df = spark.read.option("multiline", "true").json(GSMARENA_LOC + "GSMArena.json")
    catalog_df = preprocess(catalog_df)
    merge_to_db(catalog_df)
    print("Updating cell_catalog table completed!")
