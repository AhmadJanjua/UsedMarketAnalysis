import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup, PageElement
from scraperHelper import requestPage


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


# --- HELPER FUNCTIONS
def generateName(suffix: str, number_days: int) -> str:
    """
    ## Overview
    - this function generates a name containing the date and amount of data

    ## Args
    - **suffix** : *str* - a unique name for the file sent
    - **number_days** : *int* - the number of days logged

    ## Return
    - Returns a string in the format day-month-year-quantity-suffix.json
    
    """
    if number_days < 0:
        content = '-all-'
    elif number_days <= 1:
        content = '-daily-'
    else:
        content = f"-{number_days}-"

    return datetime.today().strftime("%d-%m-%Y") + content + suffix + ".json"


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
def scrapeListing(element: PageElement) -> dict | None:
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
            
            metadata['url'] = url
                
            return metadata
    
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


def addListingData(element: PageElement, DAYS: set, kijiji_data: list):
    '''
    ## Overview
    - Function validates the metadata then updates the kijiji data and returns if the date was found in the
    set of valid dates

    ## Args
    - **element** : *PageElement* - listing card with url and id information
    - **DAYS** : *set* - set of valid dates or empty set if all dates are valid
    - **kijiji_data** : *list* - list to populate with data of parsed listings

    ## Return
    - Returns bool value representing if the date was valid or not
    '''

    metadata = scrapeListing(element)

    if metadata is None:
        return True

    kijiji_data.append(metadata)

    if len(DAYS) == 0:
        return True
    
    date = metadata["Date"]

    if date in DAYS:
        return True
    else:
        return False


# -- MAIN FUNCTION
def scrapeKijiji(url : str, limit_days: int = -1) -> dict:
    '''
    ## Overview
    - This function scrapes all the listings associated with a search query up to the limit specified.

    ## Args
    - **url** : *str* - must be a formatted string with brace as placeholder for page number
    - **limit_days** : *int* - positive integer representing the number of historical days to test.
    Negative int represents no bound.

    ## Return
    - **dict** - returns a dictionary with format {listingID : metaData} where metaData is another dictionary
    '''

    page_last = False
    page_num = 1
    data = []
    DAYS = getValidDays(limit_days)

    with ThreadPoolExecutor() as pool:
        while not page_last:
            # 1. get search page
            srch_soup = requestPage(url.format(page_num), "Alert: putting main thread to sleep...")

            if srch_soup is None:
                return data

            # 2. get search result count
            page_last = getSearchResultCount(srch_soup, url.format(page_num))

            if page_last is None:
                return data

            # 3. get all listings on the page
            srch_listings = getAllListings(srch_soup, url.format(page_num))

            if srch_listings is None:
                return data
            
            # 4. process each listing using thread pool and save into data
            pool_futures = [pool.submit(addListingData, s, DAYS, data) for s in srch_listings]
            results = [f.result() for f in pool_futures]

            # Exit if the last 10 adds are beyond valid dates
            if limit_days >= 0 and not any(results[-10:]):
                break

            page_num += 1

        pool.shutdown(wait=True)

    return data
