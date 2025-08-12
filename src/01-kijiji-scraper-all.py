import requests
import json
import time
from bs4 import BeautifulSoup, PageElement
from concurrent.futures import ThreadPoolExecutor


# set to avoid issues with requests
HEADER = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36"
}


def requestPage(url: str, sleep_msg: str) -> requests.Response | None:
    '''
    ## Overview
    - Attempts to retrieve the requested page with linear back-off when too many request error comes up
    - If the page status code is other than 429 or 200, it returns none and terminates the request

    ## Args
    - **url** : *str* - url of the search page for error reporting
    - **sleep_msg** : *str* - custom message for when the request sleeps. Should be in form 'Alert: ...'

    ## Return
    - Returns the page if attained successfully, otherwise None
    '''

    status = 429
    sleep_time = 15

    while status == 429:
        page = requests.get(url, headers=HEADER)
        page.close()

        status = page.status_code

        if status == 429:
            print(sleep_msg)
            time.sleep(sleep_time)
            sleep_time += 15

        elif status != 200:
            print(f"Error: status code {status} - failed to retrieve {url}")
            return None

        else:
            return page


def getSearchResultCount(soup: BeautifulSoup, url: str) -> bool | None:
    '''
    ## Overview
    - This function takes in a search page and returns if the page is the last one
    - On error the page returns a None value

    ## Args
    - **soup** : *BeautifulSoup* - search page with result count
    - **url** : *str* - url of the search page for error reporting

    ## Return
    - Returns a bool value indicating last page, on error it returns None
    '''

    # 1. get the element containing search count
    result_count = soup.find_all("h2", attrs={"data-testid":"srp-results"})

    if len(result_count) != 2:
        print(f"Error: search page format error at {url}")
        return None
    
    # 2. extract all the numbers from the string
    result_count = result_count[0].text.split()
    result_count = [x.replace(",", "") for x in result_count]
    result_count = [int(x) for x in result_count if x.isnumeric()]

    # 3. check for issue and report
    if len(result_count) != 3:
        print(f"Error: failed to extract search result count at {url}")
        return None

    print(f"Parsing: {result_count[0]}-{result_count[1]} of {result_count[2]}")

    return result_count[1] == result_count[2]


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


def scrapeKijijiListing(element: PageElement, kijiji_data: dict):
    '''
    ## Overview
    - This function scrapes the content of listings with prices. Listings without prices (swap/trade) or 
    (please contact) are not processed.

    ## Args
    - **element** : *PageElement* - listing card from search page
    - **kijiji_data** : *dict* - dictionary of all url - metadata pairs

    ## Return
    - It updates the content of **kijiji_data** if the parsing succeeds. Returns None.
    '''

    # 1. parse the element tag for id and url
    id = element.findAll(attrs={"data-testid" : "listing-card"})
    url = element.findAll(attrs={"data-testid" : "listing-link"})

    if len(id) < 1 or len(url) < 1:
        print("Error: could not parse a listing on search page")
        return

    try:
        id = id[0].attrs["data-listingid"]
        url = url[0].attrs["href"]

    except Exception:
        print("Error: listing on search page missing attributes")
        return

    # 2. get the listing page
    listing_page = requestPage(url, "Alert: Putting thread to sleep...")

    if listing_page is None:
        return

    listing_soup = BeautifulSoup(listing_page.text, features="html.parser")

    # 3. try to extract the meta-data within the script
    listing_scripts = listing_soup.head.find_all_next("script", attrs={'type':'application/ld+json'})

    if (len(listing_scripts) != 0):
        listing_script = listing_scripts[0]

        try:
            listing_json = json.loads(listing_script.text)

            match listing_json["@type"]:
                case "Product":
                    metadata = parseProduct(id, listing_json)

                case "Motorcycle":
                    metadata = parseMotorcycle(id, listing_json)

                case _:
                    raise NameError("'@type' missing")

            kijiji_data[url] = metadata
            return
    
    # --- error reporting and handling
        except json.JSONDecodeError:
            print(f"Error: script does not contain meta-data at {url}")
            return

        except NameError:
            print(f"Error: unknown meta-data schema at {url}")
            return

        except KeyError:
            print(f"Error: meta-data missing key at {url}")
            return

        except Exception:
            print(f"Error: meta-data issue at {url}")
            return
    else:
        print(f"Error: scripts missing at {url}")
    

def scrapeKijijiSearch(url : str) -> dict:
    '''
    ## Overview
    - This function scrapes all the listings associated with a search query.

    ## Args
    - **url** : *str* - must be a formatted string with brace as placeholder for page number

    ## Return
    - **dict** - returns a dictionary with format {listingID : metaData} where metaData is another dictionary
    '''

    page_last = False
    listing_num = 1
    page_num = 1
    data = {}

    with ThreadPoolExecutor() as pool:
        while not page_last:
            # 1. get search page
            srch_page = requestPage(url.format(page_num), "Alert: putting main thread to sleep...")

            if srch_page is None:
                return data

            srch_soup = BeautifulSoup(srch_page.text, features="html.parser")

            # 2. get search result count
            page_last = getSearchResultCount(srch_soup, url.format(page_num))

            if page_last is None:
                return data

            # 3. get all listings on the page
            srch_listings = srch_soup.find_all("ul", attrs={"data-testid":"srp-search-list"})

            if len(srch_listings) < 1:
                print(f"Error: missing listing unordered list at {url.format(page_num)}")
                return data
            
            srch_listings = srch_listings[0]

            # 4. process each listing using thread pool and save into data
            for srch_listing in srch_listings:
                print(f"Parsing Listing: {listing_num}")
                listing_num += 1
                
                pool.submit(scrapeKijijiListing, srch_listing, data)
                
            page_num += 1
    
    return data


if __name__ == '__main__':
    # Example urls
    CELL_URL = "https://www.kijiji.ca/b-cell-phone/canada/page-{}/c760l0?for-sale-by=ownr&price=1__&view=list"
    BIKE_URL = "https://www.kijiji.ca/b-sport-bikes/canada/page-{}/c304l0?for-sale-by=ownr&price=1__&view=list"


    results = scrapeKijijiSearch(BIKE_URL)

    # Save to file
    with open("data/test3.json", "w") as f:
        json.dump(results, f, indent=4)