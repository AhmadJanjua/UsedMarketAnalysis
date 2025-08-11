# UsedMarketAnalysis
Full data pipeline for scraping information from market places and identifying trends.

# Setup
The project is set up using conda to manage environments and python. To run, install conda and run this command:
```
conda env create -f environment.yml
```

# Directories

## notebooks
This directory contains notebooks with verbose details to illustrate my thought process when inspecting and creating tools.


## src
This directory includes the code to execute the jobs.

### Kijiji Scraper All
This program goes through all the pages of a search and saves the data from each listing. Implemented multi-threading and achieved 10x performance limited by request block from site (30 min -> 3 minutes for 1600 listings)

**NOTE**: must have data directory or else it will not save

- It uses requests to get the search page then goes to the links attached to each listing
- uses BeautifulSoup to parse the HTML
- program is multi-threaded with sleep back-off when too many requests are reported.