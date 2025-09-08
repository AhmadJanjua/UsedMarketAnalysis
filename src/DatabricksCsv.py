import os
import dotenv
import pandas as pd
from databricks import sql

# NOTE: this function is being used since ODBC connections are not available in Tableau Public

def Databricks_to_CSV(save_dir: str, catalog: str, tables: list[str]) -> None:
    '''
    This function saves tables as csv. Should be cautious since the size may be very large.

    Args:
    ---
    - save_dir (str): directory to save the csv files in.
    - catalog (str): the catalog.schema that the tables are contained in.
    - tables (list[str]): list of the names of the tables to extract.

    Return:
    - None
    '''
    # extract the database parameters
    dotenv.load_dotenv(".env")
    HOST = os.getenv("DB_HOST")
    PATH = os.getenv("DB_PATH")
    TOK = os.getenv("DB_ACCESS")

    # connect to the database
    with sql.connect(HOST, PATH, TOK) as connection:
        with connection.cursor() as cursor:

            # save each
            for t in tables:
                print(f"Retrieving {catalog}.{t}...")
                
                # extract data
                cursor.execute(f"SELECT * FROM {catalog}.{t}")
                rows = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]

                # save to csv
                print(f"Saving to {save_dir}{t}...")
                df = pd.DataFrame(rows, columns=col_names)
                df.to_csv(f"{save_dir}{t}.csv", index=False)

                print("Complete...")


    print("Successfully closed all connections.")

if __name__ == "__main__":
    directory = "data/gold/"

    catalog = "webscraper.gold"

    tables = [
        "cost", "electronic_brand", "info", "listing",
        "location", "odometer", "storage", "vehicle_spec"
    ]

    Databricks_to_CSV(directory, catalog, tables)