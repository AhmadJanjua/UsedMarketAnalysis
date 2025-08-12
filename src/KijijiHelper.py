from bs4 import BeautifulSoup, PageElement
from datetime import date, timedelta

import requests
import json
import time


# --- CONSTANTS
_HEADER = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36"
}


# --- DICTIONARY PARSERS
def _parseProduct(id: str, metadata: dict) -> dict:
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

def _parseMotorcycle(id: str, metadata: dict) -> dict:
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


# --- HELPER METHODS

## -- General
def getValidDays(n: int):
    '''
    ## Overview
    - This function returns a set of n previous days + today and tomorrow as strings. If n
    is negative it returns an empty set.

    ## Args
    **n**: *int*: the number of previous days, set negative for empty set

    ## Return
    Returns a set of n previous days + today + tomorrow as strings or an empty set
    '''

    if n < 0: return set()
    
    days = set()
    today = date.today()
    tomorrow = today + timedelta(days=1)

    days.add(today.strftime("%Y-%m-%d"))
    days.add(tomorrow.strftime("%Y-%m-%d"))

    for i in range(1, n+1):
        cur_date = today - timedelta(days=i)
        days.add(cur_date.strftime("%Y-%m-%d"))

    return days


def requestPage(url: str, sleep_msg: str) -> BeautifulSoup | None:
    '''
    ## Overview
    - Attempts to retrieve the requested page with exponential back-off when too many request error comes up
    - If the page status code is other than 429 or 200, it returns none and terminates the request
    - After 7.5 min it terminates

    ## Args
    - **url** : *str* - url of the search page for error reporting
    - **sleep_msg** : *str* - custom message for when the request sleeps. Should be in form 'Alert: ...'

    ## Return
    - Returns the page if attained successfully, otherwise None
    '''

    status = 429
    sleep_time = 30

    while status == 429:
        page = requests.get(url, headers=_HEADER)
        page.close()

        status = page.status_code

        if status == 429:
            if sleep_time >= 480:
                print(f"Error: requesting page timeout {url}")
                return None

            print(sleep_msg)
            time.sleep(sleep_time)
            sleep_time *= 2

        elif status != 200:
            print(f"Error: status code {status} - failed to retrieve {url}")
            return None

        else:
            return BeautifulSoup(page.text, features="html.parser")


## -- Search Page
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


def getAllListings(soup: BeautifulSoup, url: str) -> PageElement | None:
    '''
    ## Overview
    - Finds the unordered list containing the listings

    ## Args
    - **soup** : *BeautifulSoup* - search page with listings
    - **url** : *str* - url of the search page for error reporting

    ## Return
    - Returns a PageElement unordered list with the search results, on error it returns None
    '''

    listings = soup.find_all("ul", attrs={"data-testid":"srp-search-list"})

    if len(listings) < 1:
        print(f"Error: missing listing unordered list at {url}")
        return
        
    return listings[0]

## -- Listing Page
def scrapeListing(element: PageElement) -> tuple[str, dict] | None:
    '''
    ## Overview
    - This function scrapes the content of listings with prices. Listings without prices (swap/trade) or 
    (please contact) are not processed.

    ## Args
    - **element** : *PageElement* - listing card from search page

    ## Return
    - It returns the url and meta data if listing was parsed successfully, otherwise it returns None
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
    listing_soup = requestPage(url, "Alert: Putting thread to sleep...")

    if listing_soup is None:
        return

    # 3. try to extract the meta-data within the script
    listing_scripts = listing_soup.head.find_all_next("script", attrs={'type':'application/ld+json'})

    if (len(listing_scripts) != 0):
        listing_script = listing_scripts[0]

        try:
            listing_json = json.loads(listing_script.text)

            match listing_json["@type"]:
                case "Product":
                    metadata = _parseProduct(id, listing_json)

                case "Motorcycle":
                    metadata = _parseMotorcycle(id, listing_json)

                case _:
                    raise NameError("'@type' missing")
                
            return (url, metadata)
    
        # --- error reporting
        except json.JSONDecodeError:
            print(f"Error: script does not contain meta-data at {url}")

        except NameError:
            print(f"Error: unknown meta-data schema at {url}")

        except KeyError:
            print(f"Error: meta-data missing key at {url}")

        except Exception:
            print(f"Error: meta-data issue at {url}")
    else:
        print(f"Error: scripts missing at {url}")


def addListingData(element: PageElement, DAYS: set, kijiji_data: dict):
    '''
    ## Overview
    - Function validates the metadata then updates the kijiji data and returns if the date was found in the
    set of valid dates

    ## Args
    - **element** : *PageElement* - listing card with url and id information
    - **DAYS** : *set* - set of valid dates or empty set if all dates are valid
    - **kijiji_data** : *dict* - dictionary with data of parsed listings

    ## Return
    - Returns bool value representing if the date was valid or not
    '''

    result = scrapeListing(element)

    if result is None: return True
    
    url, metadata = result
    date = metadata["Date"]

    kijiji_data[url] = metadata

    if len(DAYS) == 0:
        return True
    
    elif date in DAYS:
        return True
    
    else:
        return False
