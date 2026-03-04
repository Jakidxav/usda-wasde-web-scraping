# USDA WASDE web scraping

## About
The USDA's Economic Research Service (ERS) maintains consolidated, annual datasets for crop statistics like yield, planted area, harvested area, price, etc. for many staple crops like cotton, maize, and soy. For example:
 - Corn: [Feed Grains Yearbook](https://www.ers.usda.gov/data-products/feed-grains-database/feed-grains-yearbook-tables)
 - Cotton: [Cotton and Wool Yearbook](https://www.ers.usda.gov/data-products/cotton-wool-and-textile-data/cotton-and-wool-yearbook)
 - Soy: [Oil Crops Yearbook](https://www.ers.usda.gov/data-products/oil-crops-yearbook)

However, the USDA's Economics, Statistics, and Market Information System's (ESMIS) publishes monthly updates for this data and more for many other crops through its World Agricultural Supply and Demand Estimates (WASDE) site. The purpose of this repository is to show how you could scrape this data, parse it into a form that can be used with a Pandas `DataFrame`, and verify it with data from official USDA reports.

## Coding environment
To recreate the coding environment we use in this repository, please run:
```
mamba env create -f environment.yml
```
And to activate the environment:
```
mamba activate wasde_web_scraping
```