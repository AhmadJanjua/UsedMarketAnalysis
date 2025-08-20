import json
from KijijiHelper import generateName, scrapeKijiji


if __name__ == '__main__':
    # urls
    CELL_URL = "https://www.kijiji.ca/b-cell-phone/canada/page-{}/c760l0?for-sale-by=ownr&price=1__&view=list"
    BIKE_URL = "https://www.kijiji.ca/b-sport-bikes/canada/page-{}/c304l0?for-sale-by=ownr&price=1__&view=list"

    category = "cell"
    number_days = 0

    match category:
        case "cell":
            results = scrapeKijiji(CELL_URL, number_days)
        case "motorcycle":
            results = scrapeKijiji(BIKE_URL, number_days)
        case _:
            raise ValueError(f"value must be 'cell' or 'motorcycle', provided value {category}")

    # save file
    filename = "data/" + generateName(category, number_days)

    with open(filename, "w") as f:
        json.dump(results, f, indent=4)
