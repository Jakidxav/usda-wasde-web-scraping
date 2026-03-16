# USDA WASDE web scraping

## About
The USDA's Economic Research Service (ERS) maintains consolidated, annual datasets for crop statistics like yield, planted area, harvested area, price, etc. for many staple crops like corn, cotton, and soy. For example:
 - Corn: [Feed Grains Yearbook](https://www.ers.usda.gov/data-products/feed-grains-database/feed-grains-yearbook-tables)
 - Cotton: [Cotton and Wool Yearbook](https://www.ers.usda.gov/data-products/cotton-wool-and-textile-data/cotton-and-wool-yearbook)
 - Soy: [Oil Crops Yearbook](https://www.ers.usda.gov/data-products/oil-crops-yearbook)

However, the USDA's Economics, Statistics, and Market Information System's (ESMIS) publishes monthly updates for these and many other crops through its World Agricultural Supply and Demand Estimates (WASDE) site. The purpose of this repository is to show how you could scrape this monthly data, parse it into a form that can be used with a Pandas `DataFrame`, and verify it with data from official USDA reports.

## Coding environment
To recreate the coding environment we use in this repository, please run:
```
mamba env create -f environment.yml
```
And to activate the environment:
```
mamba activate wasde_data
```

## Running the code
### 1. Scrape the data for supported crops from the WASDE site
By default, both `Python` scripts in this repository will download or process data for every supported crop. For example, this command will download data from the WASDE site for `corn`, `cotton`, and `soybeans`:
```
python3 src/download_wasde_data.py
```

To run the program for individual crops, you can use the `-c` or `--crops` flag like so:
```
python3 src/download_wasde_data.py -c cotton
```

You can also run this Python script for multiple or all of the supported crops with:
```
python3 src/download_wasde_data.py
```

By default, the web scraping script will start looking for data in `1995` and then try to download all data until the current year and month. You can override this behavior by supplying arguments for the start or end year / month.

#### Example: download data for each crop for a single year
```
python3 src/download_wasde_data.py -sy 2000 -ey 2000
```

#### Example: download data for corn from May of one year to April of the next (mimicking an example commodity year)
```
python3 src/download_wasde_data.py -c corn -sy 2015 -sm 5 -ey 2016 -em 4
```

Currently, downloading data for only specific months in consecutive years (April - October for 1996, 1997, etc.) is not supported. The script will download all data in between `{start_year}-{start_month}` and `{end_year}-{end_month}`, e.g., `end_month` corresponds to the month that the script will stop processing data for `end_year`. The default value for `end_month` is `12`, as the script defaults to looking for data from January - December. While the script will not fail when looking for data that does not exist, this behavior can be easily overriden if you are analyzing data for the current year and the current month is before than December.

Lastly, this script currently suppresses printing output when monthly data is successfully downloaded. You can change this behavior to see all output with the `--show-output` flag.
```
python3 src/download_wasde_data.py -c soybeans -s True
```

### 2. Parse the downloaded `TXT` and `XLS` files
Similar to how the web scraping script works, you can parse files for individual crops or multiple / all crops. The parsing script will look for all available files to parse for a given crop, so there is no need to supply start and end year / month arguments.

For all crops:
```
python3 src/parse_wasde_files.py
```

Or for individual crops:
```
python3 src/parse_wasde_data.py -c corn soybeans
```

There is also an accompanying Jupyter Notebook that has more output printed out at each stage of the parsing pipeline.


### 3. Compare the scraped and processed data to official USDA statistics
For each crop (`corn`, `cotton`, `soybeans`), there is a Jupyter Notebook showing how the parsed data compares to official USDA statistics used for figures on the [National Agricultural Statistics Service](https://www.nass.usda.gov/Charts_and_Maps/Field_Crops) (NASS) site. For each crop, you can see that these web scraping and processing functions produce the correct data for planted and harvested area, yield, and (when a source is available to check) abandonment rate.
