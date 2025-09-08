import pyspark.sql.functions as F
from delta.tables import DeltaTable
import sys

def merge_listing(df):
    """
    Merge dataframe to listings table. Updates existing listings and appends new

    ### Args
    - df (pyspark.DataFrame): processed table of listings

    ### Return
    - None
    """
    dt = DeltaTable.forName(spark, "webscraper.gold.listing")
    dt\
        .alias("l")\
        .merge(df.alias("r"), "l.id = r.id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

def merge_scd(table: str, df):
    """
    Merge function for slow changing dimension tables. On match, invalidates existing and appends new.

    ### Args
    - table (str): name of the table to merge into
    - df (pyspark.DataFrame): table to merge into database

    ### Return
    - None
    """
    timestamp = spark.sql("select current_timestamp() ts").collect()[0]["ts"]
    dt = DeltaTable\
        .forName(spark, table)\
        .alias("l")

    df = df \
        .withColumn("valid_from", F.lit(timestamp)) \
        .withColumn("valid_to", F.lit(None).cast("timestamp")) \
        .alias("r")

    dt\
        .merge(df,
        "l.id = r.id AND l.valid_to IS NULL AND l.hash <> r.hash") \
        .whenMatchedUpdate(set={ "valid_to": F.col("r.valid_from") }) \
        .execute()

    dt\
        .merge(df,
        "l.id = r.id AND l.valid_to IS NULL")\
        .whenNotMatchedInsertAll() \
        .execute()

def fmt_text(x):
    """
    Trims and makes whitespaces uniform.

    ### Args
    - x (Columnn): a column of strings to be formatted.

    ### Return
    - Returns the text with even spaces, no leading or trailing spaces.
    """
    return F.trim(F.regexp_replace(x, r"\s+", " "))

def norm_text(x):
    """
    Normalizes the text with uniform spacing, and lower case ascii
    alphabet without special characters.

    ### Args
    - x (Column): column with text to be normalized

    # Return
    - Returns text completely normalized
    """
    l = F.lower(x)
    l = F.regexp_replace(l, r"\+", " plus ")
    l = F.regexp_replace(l, r"[^a-z0-9]+", " ")
    l = F.regexp_replace(l, r"\btags.*$", "")
    l = F.regexp_replace(l, r"\s+", " ")
    return F.trim(l)

def sha256(cols):
    """
    Converts each row into a sha256 hash using the specified columns

    ### Args
    - cols (list[str]): list of column names to be hashed

    ### Return
    - A hash of the columns provided
    """
    cols = [F.coalesce(F.col(c).try_cast("string"), F.lit("null")) for c in cols]
    return F.sha2(F.concat_ws("|",*cols), 256)

def clean_listings(df) -> tuple:
    '''
    This table describes what should be in other tables,
    first filter through this table and extract the remaining
    columns based on that.

    ### Args
    - df (pyspark.DataFrame): listings data to be filtered out for the main listing table.

    ### Return
    - Returns a tuple of dataframes. The first value is the dataframe that other tables will
    be made from, the second will be the listings dataframe.
    '''
    df = (
        df
        .withColumn("id", F.col("id").try_cast("long"))
        .withColumn("category", F.col("category").try_cast("string"))
        .withColumn("category", fmt_text(F.col("category")))
        .withColumn("posted_at", F.col("date").try_cast("date"))
        .withColumn("processed_at", F.col("processed_at").try_cast("timestamp"))
        .dropna(subset=["id", "category", "posted_at", "processed_at"])
        .dropDuplicates(["id"])
    )

    listing = df.select("id", "category", "posted_at", "processed_at")
    og = df.drop("category", "posted_at", "date", "processed_at")
    return og, listing 

def clean_info(df):
    """
    Preprocesses the gold info table.

    ### Args
    - df (pyspark.DataFrame): table must include id, info column with title and description.

    ### Return
    - Processed info dataframe.
    """
    return (
        df
        .select("id", "info.title", "info.description")
        .withColumn("title", F.col("title").try_cast("string"))
        .withColumn("description", F.col("description").try_cast("string"))
        .withColumn("title", fmt_text(F.col("title")))
        .withColumn("description", fmt_text(F.col("description")))
        .withColumn("title",
            F.when(F.col("title") == "", F.lit(None)).otherwise(F.col("title")))
        .withColumn("description",
            F.when(F.col("description") == "", F.lit(None)).otherwise(F.col("description")))
        .dropna(subset=["title"])
        .withColumn("hash", sha256(["title", "description"]))
    )

def clean_location(df):
    """
    Preprocesses the location table. Extracts postal code.

    ### Args
    - df (pyspark.DataFrame): Dataframe with id, location(country, city, address, coordinates(lat, lon)).

    ### Return
    - Returns a processed dataframe.
    """
    postal_regex = r"(?i)\b(([A-Z][0-9][A-Z])\s?([0-9][A-Z][0-9])?)\b"

    return (
        df
        # get subset
        .select(
            "id",
            "location.country",
            "location.city",
            "location.address",
            "location.coordinate.lat",
            "location.coordinate.lon"
        )

        # type casting
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("country", F.col("country").try_cast("string"))
        .withColumn("city", F.col("city").try_cast("string"))
        .withColumn("address", F.col("address").try_cast("string"))
        .withColumn("lat", F.col("lat").try_cast("double"))
        .withColumn("lon", F.col("lon").try_cast("double"))

        # extract postal code
        .withColumn("postal_code", F.regexp_extract(F.col("address"), postal_regex, 1))

        # remove postal code from address
        .withColumn("address", F.regexp_replace(F.col("address"), F.col("postal_code"), ""))

        # format address
        .withColumn("address", fmt_text(F.col("address")))
        .withColumn("address", F.regexp_replace(F.col("address"), r",$", ""))

        # format postal code
        .withColumn("postal_code", fmt_text(F.upper(F.overlay("postal_code", F.lit(" "), 4, 0))))
        # replace blank with null
        .withColumn("postal_code",
            F.when(F.col("postal_code") == "", F.lit(None)).otherwise(F.col("postal_code")))
        
        # replace blank with postal code
        .withColumn("address",
            F.when(F.col("address") == "", F.col("postal_code")).otherwise(F.col("address")))
        
        # check constraints
        .filter("lat IS NOT NULL AND lat BETWEEN -90 AND 90")
        .filter("lon IS NOT NULL AND lon BETWEEN -180 AND 180")
        .filter("country IS NOT NULL AND country <> ''")
        .filter("city IS NOT NULL AND city <> ''")
        .filter("address IS NOT NULL AND address <> ''")
        .filter("postal_code IS NULL OR postal_code <> ''")

        # hash for easy lookup
        .withColumn("hash", sha256(["country", "city", "address", "postal_code", "lat", "lon"]))
    )

def clean_cost(df):
    """
    Preprocesses the gold cost table.
    
    ### Args
    - df (pyspark.DataFrame): Frame with id, cost(currency, price) columns.

    ### Return
    - Processed dataframe cleaned to be merged into the gold.cost table.
    """
    return (
        df
        # collect subset
        .select("id", "cost.currency", "cost.price")

        # cast type
        .withColumn("currency", F.col("currency").try_cast("string"))
        .withColumn("price", F.col("price").try_cast("decimal(18,2)"))

        # process currency
        .fillna("CAD", subset=["currency"])
        .filter(F.length(F.col("currency")) == 3)

        # process price
        .fillna(0, subset=["price"])
        .filter(F.col("price") >= 0)

        # hash
        .withColumn("hash", sha256(["currency", "price"]))
    )

def clean_electronic_brand(df):
    """
    Reads the title and description and attempts to parse the cellphone brand and model.
    
    ### Args
    - df (pyspark.DataFrame): Table with id, info(title, description)

    ### Return
    - Returns a table of id, brand, models as a result of data extraction
    """
    def attach_brand(b_df):
        """
        This function attaches the brand
        """
        brands = spark.table("webscraper.silver.cell_brands")
        b_df = b_df.crossJoin(brands)

        # 1. Match the patterns against text
        b_df = b_df.withColumn("match_pos", F.regexp_instr("text", "pattern"))
        # filter out non-matches
        b_df = b_df.where(F.col("match_pos") != 0)

        # 2. Create table to identify first match
        first_match = b_df.groupBy("id").agg(
            F.min("match_pos").alias("min"),
            F.count("brand").alias("count"))
        
        # filter out ads with many brands (usually repair or professional)
        first_match = first_match.filter(F.col("count") <= 3)

        # 3. Keep only the first match
        b_df = b_df.join(first_match, on="id", how="right_outer")
        b_df = b_df.filter(F.col("min") == F.col("match_pos"))

        return (
            b_df
            .select("id", "brand")
            .dropDuplicates()
        )

    def attach_model(m_df):
        """
        This function attaches the model based on the brand
        """
        models = spark.table("webscraper.silver.cell_models")
        models = models.select("brand", "model", "pattern", "length")
        m_df = m_df.join(models, "brand", "inner")

        # match pattern
        m_df = m_df.withColumn("match_pos", F.regexp_instr("text", "pattern"))
        # filter out non-matches
        m_df = m_df.filter(F.col("match_pos") != 0)

        # 2. filter out ads with too many matches (likely repairs)
        repair_df = m_df.groupBy("id").count()
        repair_df = repair_df.filter(F.col("count") < 10)
        m_df = m_df.join(repair_df, "id", "inner")

        # 3. get the longest match
        longest_df = m_df.groupBy("id").agg(F.max(F.col("length")).alias("max"))
        m_df = m_df.join(longest_df, "id", "inner")
        m_df = m_df.filter(F.col("max") == F.col("length"))

        # 4. get first match
        first_df = m_df.groupBy("id").agg(F.min(F.col("match_pos")).alias("min"))
        m_df = m_df.join(first_df, "id", "inner")
        m_df = m_df.filter(F.col("min") == F.col("match_pos"))

        return (
            m_df
            .select("id", "model")
            .dropDuplicates()
        )
    
    df = (
        df
        .select("id", "info.title", "info.description")
        .withColumn("text", F.concat_ws(" ", norm_text(F.col("title")), norm_text(F.col("description"))))
        .drop("title", "description")
    )
    df = df.join(attach_brand(df), on="id", how="left_outer")
    df = df.join(attach_model(df), on="id", how="left_outer")
    
    return (
        df
        .select("id", "brand", "model")
        .filter("brand IS NOT NULL AND brand <> ''")
        .withColumn("hash", sha256(["brand", "model"]))
    )

def clean_storage(df):
    """
    This function reads the title and description and attempts to extract the storage from it.

    ### Args
    - df (pyspark.DataFrame): Requires table with id, info.title, info.description

    ### Return
    - Returns a table with storage dataframe.
    """
    # 1. create patterns with varying level of priority
    storage_patterns = spark.createDataFrame([
        # most common phone storage (128-512)
        {"storage_unit": "GB","pattern": r"\b(?!0{3})(\d{3})\s*(gb|gigs|gib)\b", "priority": 1},
        # another common storage (32,64)
        {"storage_unit": "GB","pattern": r"\b(?!0{2})(32|64)\s*(gb|gigs|gib)\b", "priority": 2},
        # common, but also used for expandable storage
        {"storage_unit": "TB","pattern": r"\b([1-8])\s*(tb)\b", "priority": 3},
        {"storage_unit": "GB","pattern": r"\b(?!0{4})(\d{4})\s*(gb|gigs|gib)\b", "priority": 4},
        # French abbreviation
        {"storage_unit": "GB","pattern": r"\b(?!0{3,4})(\d{3,4})\s*(go)\b", "priority": 5},
        # larger numbers and weak abbreviation
        {"storage_unit": "GB","pattern": r"\b(?!0{3,4})(128|256|512|1000|1024|2000|2048)\s*(g)\b", "priority": 6},
        # usually storage but can be RAM
        {"storage_unit": "GB","pattern": r"\b(?!0{2})(\d{2})\s*(gb|gigs|gib)\b", "priority": 7},
        {"storage_unit": "GB","pattern": r"\b(?!0{2})(\d{2})\s*(go)\b", "priority": 8},
        # generally RAM
        {"storage_unit": "GB","pattern": r"\b([1-9])\s*(gb|gigs|gib)\b", "priority": 9},
        {"storage_unit": "GB","pattern": r"\b([1-9])\s*(go)\b", "priority": 10},
        # weakest match
        {"storage_unit": "GB","pattern": r"\b(?!0{2,4})(\d{2,4})\s*(g)\b", "priority": 11},
        {"storage_unit": "GB","pattern": r"\b([126789])\s*(g)\b", "priority": 12},
    ])

    # 2. Create a table with each pattern and entry
    df = (
        df
        .select("id", "info.title", "info.description")
        .withColumn("text", F.concat_ws(" ", norm_text(F.col("title")), norm_text(F.col("description"))))
        .drop("title", "description")
        .crossJoin(storage_patterns)
        .withColumn("storage_amt", F.regexp_extract("text", F.col("pattern"), 1))
        .filter(F.col("storage_amt") != "")
    )

    # 3. Find highest priority for each ID
    storage_priority = df.groupBy("id").agg(F.min("priority").alias("min"))
    # join priority and filter
    df = df.join(storage_priority, "id", "inner")
    df = df.filter(F.col("min") == F.col("priority"))

    # 4. normalize all the units
    df = df.withColumn(
        "storage_gb",
        F.when(
            F.col("storage_unit") == F.lit("TB"),
            F.col("storage_amt").cast("int") * F.lit(1024)
        ).otherwise(
            F.col("storage_amt").cast("int")
    ))

    return (
        df
        .select("id", "storage_gb")
        .filter("storage_gb IS NOT NULL")
        .filter("storage_gb > 0")
        .withColumn("hash", sha256(["storage_gb"]))
    )

def clean_vehicle_spec(df):
    """
    Preprocess the gold.vehicle_spec table.
    
    ### Args
    - df (pyspark.DataFrame): Table containing model column with brand, name and year subcolumns

    ### Return
    - Preprocessed gold table with duplicates removed and constraints enforced.
    """
    return (
        df
        .select("id", "model.brand", "model.name", "model.year")
        .withColumn("brand", F.col("brand").try_cast("string"))
        .withColumn("model", F.col("name").try_cast("string"))
        .withColumn("year", F.col("year").try_cast("int"))
        .filter("year BETWEEN 1900 AND YEAR(CURRENT_DATE)+1")
        .filter("brand IS NOT NULL")
        .filter("model IS NOT NULL")
        .withColumn("hash", sha256(["brand", "model", "year"]))
        .select("id","brand", "model", "year", "hash")
    )

def clean_odometer(df):
    """
    Preprocesses the milage associated with vehicles

    ### Args
    - df (pyspark.DataFrame): table with id, odometer(unit, value)

    ### Return
    - Returns processed odometer data
    """
    return (
        df
        .select("id","odometer.unit", "odometer.value")
        .withColumn("units", F.col("unit").try_cast("string"))
        .withColumn("units", F.lower("units"))
        .withColumn("odometer", F.col("value").try_cast("int"))
        .dropna(how="any")
        .filter("units IN ('km', 'mi')")
        .filter("odometer >= 0")
        .withColumn("hash", sha256(["units", "odometer"]))
        .select("id", "units", "odometer", "hash")
    )

def cell_pipeline():
    """
    This function runs cell pipeline. Calls the functions in correcct order to update all tables and merge
    into database.

    Note: only runs the most recent changes.
    """
    df = spark.table("webscraper.silver.cell_listings")
    latest = df.agg(F.max("processed_at").alias("x")).collect()[0]["x"]
    df = df.filter(F.col("processed_at") == latest)

    # extract and clean data
    df, listing_df = clean_listings(df)
    info_df = clean_info(df)
    loc_df = clean_location(df)
    cost_df = clean_cost(df)
    brand_df = clean_electronic_brand(df)
    storage_df = clean_storage(df)

    # merge into database
    merge_listing(listing_df)
    merge_scd("webscraper.gold.info", info_df)
    merge_scd("webscraper.gold.location", loc_df)
    merge_scd("webscraper.gold.cost", cost_df)
    merge_scd("webscraper.gold.electronic_brand", brand_df)
    merge_scd("webscraper.gold.storage", storage_df)

def moto_pipeline():
    """
    Orchestrates the motorcycle data retrieval, processing and merging into the database.
    
    Note: Only runs the most recent changes.
    """
    df = spark.table("webscraper.silver.motorcycle_listings")
    latest = df.agg(F.max("processed_at").alias("x")).collect()[0]["x"]
    df = df.filter(F.col("processed_at") == latest)

    # process and create tables
    df, listing_df = clean_listings(df)
    info_df = clean_info(df)
    loc_df = clean_location(df)
    cost_df = clean_cost(df)
    spec_df = clean_vehicle_spec(df)
    odom_df = clean_odometer(df)

    # merge into database
    merge_listing(listing_df)
    merge_scd("webscraper.gold.info", info_df)
    merge_scd("webscraper.gold.location", loc_df)
    merge_scd("webscraper.gold.cost", cost_df)
    merge_scd("webscraper.gold.vehicle_spec", spec_df)
    merge_scd("webscraper.gold.odometer", odom_df)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError("Error: missed pipeline argument.")

    pipeline = sys.argv[1]

    if pipeline == "cell":
        cell_pipeline()
    elif pipeline == "motorcycle":
        moto_pipeline()