import json
import sys
from KijijiHelper import generateName, scrapeKijiji

if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise ValueError("Arguments should be 'cell'/'motorcycle' and number of days '0'")
    
    category = sys.argv[1]
    number_days = int(sys.argv[2])
    storage = sys.argv[3]


    # urls
    CELL_URL = "https://www.kijiji.ca/b-cell-phone/canada/page-{}/c760l0?for-sale-by=ownr&price=1__&view=list"
    BIKE_URL = "https://www.kijiji.ca/b-sport-bikes/canada/page-{}/c304l0?for-sale-by=ownr&price=1__&view=list"

    match category:
        case "cell":
            results = scrapeKijiji(CELL_URL, number_days)
        case "motorcycle":
            results = scrapeKijiji(BIKE_URL, number_days)
        case _:
            raise ValueError(f"value must be 'cell' or 'motorcycle', provided value {category}")

    # get prefix
    if storage == "local":
        prefix = "data/"
    elif storage == "databricks":
        if category == "cell":
            prefix = "/Volumes/webscraper/bronze/cellphone_raw/Kijiji/"
        elif category == "motorcycle":
            prefix = "/Volumes/webscraper/bronze/motorcycle_raw/Kijiji/"

    filename = prefix + generateName(category, number_days)

    with open(filename, "w") as f:
        json.dump(results, f, indent=4)
