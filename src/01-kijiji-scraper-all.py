import requests
import json
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# set to avoid issues with requests
HEADER = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36"
}

def parseProduct(id: str, metadata: dict) -> dict:
    return {
        "ID" : id,
        "Category" : "Product",
        "Title" : metadata['name'],
        "Description" : metadata['description'],
        "Price" : float(metadata['offers']['price']),
        "Currency" : metadata['offers']['priceCurrency'],
        "Date" : metadata['offers']['validFrom'],
        "Address" : metadata['offers']['availableAtOrFrom']['address']['streetAddress'],
        "City" : metadata['offers']['availableAtOrFrom']['address']['addressLocality'],
        "Country" : metadata['offers']['availableAtOrFrom']['address']['addressCountry'],
        "Latitude": float(metadata['offers']['availableAtOrFrom']['latitude']),
        "Longitude" : float(metadata['offers']['availableAtOrFrom']['longitude'])
    }


def parseMotorcycle(id: str, metadata: dict) -> dict:
    return {
        "ID" : id,
        "Category": 'Motorcycle',
        "Title" : metadata['name'],
        "Description" : metadata['description'],
        "Price" : float(metadata['offers']['price']),
        "Currency" : metadata['offers']['priceCurrency'],
        "Date" : metadata['offers']['validFrom'],
        "Address" : metadata['offers']['availableAtOrFrom']['address']['streetAddress'],
        "City" : metadata['offers']['availableAtOrFrom']['address']['addressLocality'],
        "Country" : metadata['offers']['availableAtOrFrom']['address']['addressCountry'],
        "Latitude": float(metadata['offers']['availableAtOrFrom']['latitude']),
        "Longitude" : float(metadata['offers']['availableAtOrFrom']['longitude']),
        "Brand" : metadata["brand"]["name"],
        "Model" : metadata["model"],
        "Year" : metadata["vehicleModelDate"],
        "Odometer": metadata["mileageFromOdometer"]["value"],
        "OdometerUnits" : metadata["mileageFromOdometer"]["unitCode"],
    }


def scrapeKijijiListing(url : str, id: str, kijiji_data) -> dict | None:
    '''
    ## Overview
    This function scrapes the content of listings with prices. Listings without prices (swap/trade) or 
    (please contact) are not processed.
    ## Args
    **url** : *str* - target listing page with price
    ## Return
    **dict** - returns a dictionary with meta data related to a listing
    '''
    # get the page
    listing_page = requests.Response()
    status_code = 429
    time_asleep = 15

    while status_code == 429:
        listing_page = requests.get(url, headers=HEADER)
        listing_page.close()
        status_code = listing_page.status_code

        if status_code == 429:
            print("Putting thread to sleep...")
            time.sleep(time_asleep)
            time_asleep += 15
        elif status_code != 200:
            print(f"Error: status code {status_code} - failed to retrieve listing at {url}")
            return
        else:
            break

    listing_soup = BeautifulSoup(listing_page.text, features="html.parser")

    # go through all the scripts and find the one with meta data
    listing_scripts = listing_soup.head.find_all_next("script", attrs={'type':'application/ld+json'})
    for listing_script in listing_scripts:
        try:
            metadata = {}
            listing_json = json.loads(listing_script.text)

            match listing_json["@type"]:
                case "Product":
                    metadata = parseProduct(id, listing_json)
                case "Motorcycle":
                    metadata = parseMotorcycle(id, listing_json)
                case _:
                    raise IndexError("@type missing")

            kijiji_data[url] = metadata

            return
        except Exception as e:
            continue

    print(f"Error: failed to parse the content of page at {url}")
    return
    

def scrapeKijijiSearch(url : str) -> dict:
    '''
    ## Overview
    This function scrapes all the listings associated with a search query.
    ## Args
    **url** : *str* - must be a formatted string with brace as placeholder for page number
    ## Return
    **dict** - returns a dictionary with format {listingID : metaData} where metaData is another dictionary
    '''
    page_last = False
    listing_num = 1
    page_num = 1
    data = {}

    with ThreadPoolExecutor() as pool:
        while not page_last:
            # get search page            
            status = 429
            srch_listings = requests.Response()
            sleep_time = 15
            while (status == 429):
                srch_page = requests.get(url.format(page_num), headers=HEADER)
                srch_page.close()
                status = srch_page.status_code

                if status == 429:
                    print("Putting main thread to sleep")
                    time.sleep(sleep_time)
                    sleep_time += 15
                elif srch_page.status_code != 200:
                    print(f"Error: unexpected status code at {url.format(page_num)}")
                    return data
                else:
                    break

            srch_soup = BeautifulSoup(srch_page.text, features="html.parser")


            # get the page count
            result_count = srch_soup.find_all("h2", attrs={"data-testid":"srp-results"})

            if len(result_count) != 2:
                print(f"Error: missing listing count at {url.format(page_num)}")
                return data

            result_count = result_count[0].text.split()
            result_count = [x.replace(",", "") for x in result_count]
            result_count = [int(x) for x in result_count if x.isnumeric()]

            page_last = result_count[1] == result_count[2]
            print(f"Parsing: {result_count[0]}-{result_count[1]} of {result_count[2]}")

            # get all listings on the page
            srch_listings = srch_soup.find_all("ul", attrs={"data-testid":"srp-search-list"})

            if len(srch_listings) == 0:
                print(f"Error: missing listing unoredered list at {url.format(page_num)}")
                return data
            
            srch_listings = srch_listings[0]


            for srch_listing in srch_listings:
                # get listing info
                listing_id = srch_listing.findAll(attrs={"data-testid" : "listing-card"})[0] \
                                .attrs["data-listingid"]

                listing_link = srch_listing.findAll(attrs={"data-testid" : "listing-link"})[0] \
                                .attrs["href"]
                
                pool.submit(scrapeKijijiListing, listing_link, listing_id, data)
                
                print(f"Parsing Listing: {listing_num}")
                listing_num += 1

            page_num += 1
    
    return data


if __name__ == '__main__':
    CELL_URL = "https://www.kijiji.ca/b-cell-phone/canada/page-{}/c760l0?for-sale-by=ownr&price=1__&view=list"
    BIKE_URL = "https://www.kijiji.ca/b-sport-bikes/canada/page-{}/c304l0?for-sale-by=ownr&price=1__&view=list"
    
    results = scrapeKijijiSearch(BIKE_URL)
    with open("data/kijiji_sport_bike.json", "w") as f:
        json.dump(results, f, indent=4)