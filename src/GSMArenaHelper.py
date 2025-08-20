import re
from bs4 import BeautifulSoup
from scraperHelper import requestPage
from concurrent.futures import ThreadPoolExecutor

# -- CONSTANTS
BASE_URL = "https://www.gsmarena.com/"


# -- HELPER FUNCTIONS
def getModelPages(soup: BeautifulSoup) -> list: 
    '''
    ## Overview
    - This function gets all the pages associated with a brands models

    ## Args
    - **soup** : *BeautifulSoup* - phone brand model page as BeautifulSoup object

    ## Return
    - **list** - returns a list of the url for all pages
    '''

    try:
        nav_elm = soup \
            .find("div", attrs={'class': 'nav-pages'}) \
            .find_all("a")
    except:
        return []
    
    brand_urls = []
    
    for page_num in nav_elm:
        try:
            url = page_num.attrs["href"]
        except:
            continue

        if page_num.text.isnumeric():
            brand_urls.append(url)

    return brand_urls


def getModelNames(page: BeautifulSoup|str, data: list, brand: str):
    '''
    ## Overview
    - Extracts the models on the cellphone maker page and adds them to the data list

    ## Args
    - **page** : *BeautifulSoup|str* - the url of the page or parsed page as a Beautiful soup object
    - **data** : *list* - data containing dictionary of all the brands and models to add to
    - **brand** : *str* - name of the brand associated with the phone models
    '''

    if type(page) == str:
        soup = requestPage(page)
        if soup is None: return
    elif type(page) == BeautifulSoup:
        soup = page
    else:
        return
    
    try:
        models_elm = soup \
            .find("div", attrs={"class": "makers"}) \
            .find_all("li")
    except:
        return

    for model in models_elm:
        try:
            url = model.find("a").attrs["href"]
        except:
            continue

        data.append({
            "brand": brand,
            "make": model.text,
            "url": url
        })


def getAllBrands() -> dict:
    '''
    ## Overview
    - This function returns all the brands found and their url

    ## Return
    - **dict** - returns a dictionary containing dict[*name*] = {url: *str*, quantity: *str*}
    '''

    brand_soup = requestPage("https://www.gsmarena.com/makers.php3")
    
    if brand_soup is None:
        return {}
    
    try:
        cell_elms = brand_soup \
            .find("div", attrs={"class": "st-text"}) \
            .find("table") \
            .findAll("td")

    except:
        return {}

    PATTERN = r">([^<]+)<br/><span>(\d+).+</span>"

    all_brands = {}

    for cell in cell_elms:
        matcher = re.search(PATTERN, str(cell))
        
        if matcher is None:
            print(f"Error: couldnt find match:\n{cell}")
            continue

        try:
            url = cell.find("a").attrs["href"]
        except:
            print(f"Error: couldn't extract url")
            continue
        
        brand, quantity = matcher.groups()

        all_brands[brand] = {"url": url, "quantity": int(quantity)}
    
    return all_brands

# -- MAIN FUNCTION
def scrapeGSMArena(data: list):
    '''
    ## Overview
    - Collects data from GSMArena website and saves as a list of dicts{brand, model, url}

    ## Args
    - **data** : *list* - list to add dictionary entries to
    '''

    # 1. get all the names and urls
    all_brands = getAllBrands()

    with ThreadPoolExecutor() as pool:
        for brand, info in all_brands.items():
            # 2. get the first page and all urls
            first_soup = requestPage(BASE_URL + info["url"])
            if first_soup is None:
                continue

            pages_urls = getModelPages(first_soup)

            # 3. get all models
            getModelNames(first_soup, data, brand)
            results = [pool.submit(getModelNames, BASE_URL + url, data, brand) for url in pages_urls]

            # wait for threads to finish
            [x.result() for x in results]
