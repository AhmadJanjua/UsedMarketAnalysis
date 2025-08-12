import json
from concurrent.futures import ThreadPoolExecutor
from KijijiHelper import getSearchResultCount, requestPage, getAllListings, getValidDays, addListingData

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
    data = {}
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


if __name__ == '__main__':
    # Example urls
    CELL_URL = "https://www.kijiji.ca/b-cell-phone/canada/page-{}/c760l0?for-sale-by=ownr&price=1__&view=list"
    BIKE_URL = "https://www.kijiji.ca/b-sport-bikes/canada/page-{}/c304l0?for-sale-by=ownr&price=1__&view=list"


    results = scrapeKijiji(BIKE_URL, 0)

    # Save to file
    with open("data/test2.json", "w") as f:
        json.dump(results, f, indent=4)