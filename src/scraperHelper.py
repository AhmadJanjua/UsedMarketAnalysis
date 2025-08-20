import time
import requests
from bs4 import BeautifulSoup

# --- Constants
_HEADER = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36"
}


# --- HELPER FUNCTIONS
def requestPage(
        url: str, sleep_msg: str="Alert: sleeping...", max_sleep: int=300, MAX_RETRIES: int=10
    ) -> BeautifulSoup | None:
    '''
    ## Overview
    - Attempts to retrieve the requested page with back-off when 'too many request' error comes up
    - If the page status code is other than 429 or 200, it returns none and terminates the request
    - After max_sleep seconds or MAX_RETRIES attempts, it terminates (min of both)

    ## Args
    - **url** : *str* - url of the search page for error reporting
    - **sleep_msg** : *str* - custom message for when the request sleeps. Should be in form 'Alert: ...'
        - **default**: 'Alert: sleeping'
    - **max_sleep** : maximum allowed sleep time in seconds
        - **default**: 300
    - **MAX_RETIRES**: nuber of attempt made to get the page
        - **default**: 10

    ## Return
    - Returns the page if attained successfully, otherwise None
    '''

    retries = 0

    while retries < MAX_RETRIES:
        with requests.get(url=url, headers=_HEADER) as page:
            match page.status_code:
                case 200:
                    return BeautifulSoup(page.text, features="html.parser")

                case 429:
                    try:
                        sleep_time = int(page.headers["Retry-After"])

                    except:
                        sleep_time = min(30, max_sleep)
                    
                    if sleep_time > max_sleep or max_sleep <= 0:
                        print(f"Error: page request timeout at {url}")
                        return None
                    
                    retries += 1
                    max_sleep -= sleep_time

                    print(sleep_msg)
                    time.sleep(sleep_time)

                case _:
                    print(f"Error: status code {page.status_code} - failed to retrieve {url}")
                    return None
                
    print(f"Error: max number of retries reached at {url}")
    return None
