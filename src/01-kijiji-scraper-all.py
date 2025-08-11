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

    while status_code == 429:
        listing_page = requests.get(url, headers=HEADER)
        listing_page.close()
        status_code = listing_page.status_code

        if status_code == 429:
            print("Putting thread to sleep...")
            time.sleep(10)
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
            listing_json = json.loads(listing_script.text)
            metadata = {
                "Title" : listing_json['name'],
                "Description" : listing_json['description'],
                "Price" : float(listing_json['offers']['price']),
                "Currency" : listing_json['offers']['priceCurrency'],
                "Date" : listing_json['offers']['validFrom'],
                "Address" : listing_json['offers']['availableAtOrFrom']['address']['streetAddress'],
                "City" : listing_json['offers']['availableAtOrFrom']['address']['addressLocality'],
                "Country" : listing_json['offers']['availableAtOrFrom']['address']['addressCountry'],
                "Latitude": float(listing_json['offers']['availableAtOrFrom']['latitude']),
                "Longitude": float(listing_json['offers']['availableAtOrFrom']['longitude'])
            }

            kijiji_data[id] = metadata
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
            print(f"Parsing Page: {page_num}")

            status = 429
            srch_listings = requests.Response()

            while (status == 429):
                srch_page = requests.get(url.format(page_num), headers=HEADER)
                srch_page.close()
                status = srch_page.status_code

                if status == 429:
                    print("Putting main thread to sleep")
                    time.sleep(10)
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
    CELL_URL = "https://www.kijiji.ca/b-cell-phone/alberta/page-{}/c760l9003?for-sale-by=ownr&view=list"
    results = scrapeKijijiSearch(CELL_URL)

    with open("data/kijiji_thread.json", "w") as f:
        json.dump(results, f, indent=4)