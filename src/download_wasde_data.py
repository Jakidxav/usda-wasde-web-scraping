#!/usr/bin/env python3
"""
This script is used to scrape USDA WASDE txt and XLS files for corn, cotton, and soybeans
data from Jan 1995 - Jan 2026. There is a fallback filename pattern, since many of
the files over the years have changed naming conventions. There is strict validation that 
the file belongs to the queried month and year.

If no suitable .txt file is found, the script then tries to download a XLS file that follows 
the same naming conventions. When an XLS is found, only the worksheet(s) that contain the 
crop specific supply regex are saved. I found that the XLS files were much simpler to work with 
than the XML and PDF files. I was able to download PDFs and extract text, but the end result
formatting was basically unusable.
"""

import argparse
from datetime import datetime
import io
import os
import re
import requests
import sys

from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm
from urllib.parse import urljoin


def month_range(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    Yield (year, month) tuples from  `{start_month}-{start_year}` through `{end_month}-{end_year}` (both inclusive).

    Example:
        >>> list(month_range(2023, 2023))
        [(2023, 1), (2023, 2), (2023, 3), (2023, 4), (2023, 5),
         (2023, 6), (2023, 7), (2023, 8), (2023, 9), (2023,10),
         (2023,11), (2023,12)]
    """
    # Build a pandas DatetimeIndex that starts on 1‑Jan‑start_year
    # and ends on 1‑Dec‑end_year (the “MS” frequency gives the first day
    # of each month).  Using `normalize()` ensures we have midnight timestamps.
    assert(end_year >= start_year)

    dates = pd.date_range(
        start=pd.Timestamp(year=start_year, month=start_month, day=1),
        end=pd.Timestamp(year=end_year, month=end_month, day=1),
        freq="MS",
    )

    # yield (year, month) tuples in chronological order
    for dt in dates:
        yield dt.year, dt.month


def build_query_url(base: str, year: int, month: int) -> str:
    """
    Create a date-specific URL with ?date=YYYY-MM
    """
    
    return f"{base}?date={year:04d}-{month:02d}"


def resolve_absolute(base: str, href: str) -> str:
    """
    Make a possibly‑relative href absolute.
    """
    
    return urljoin(base, href)


def download_text(url: str) -> str:
    """
    GET the raw .txt file and return its decoded content.
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    
    return resp.text


def download_binary(url: str) -> bytes:
    """
    GET a binary file (used for XLS) and return its raw bytes.
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    
    return resp.content


def extract_crop_section(crop: str, text: str, lines_before_start: int = 2) -> str | None:
    """
    Return the block that:
      • Begins *lines_before_start* full lines **before** the first occurrence
        of `start_text_pattern`. In general, there are 2-3 lines of text including USDA
        report numbers that I would like to keep in the raw text files for validation.
      • Ends **just before** the first occurrence of `end_text_pattern` that appears
        after the start marker.

    If either marker cannot be found, `None` is returned.
    """
    if crop == 'corn':
        # corn regex pattern starts with 'feed grain and corn' and *normally* ends with 'sorghum, barley(,) and oats'
        start_pattern = re.compile(r'U\.?\s*\.?S\.?\s*Feed\s+Grain\s+and\s+Corn\s+Supply\s+and\s+Use\s+1', re.IGNORECASE)
        end_pattern   = re.compile(r'U\.?\s*\.?S\.?\s*Sorghum,\s+Barley,?\s+and\s+Oats\s+Supply\s+and\s+Use\s+1', re.IGNORECASE)
    elif crop == 'cotton':
        # cotton regex pattern starts with 'cotton' and *normally* ends with 'world wheat'
        start_pattern = re.compile(r'U\.?\s*\.?S\.?\s*Cotton\s+Supply\s+and\s+Use\s+1', re.IGNORECASE)
        end_pattern   = re.compile(r'World\s+Wheat\s+Supply\s+and\s+Use\s+1/?', re.IGNORECASE)
    else: # crop == 'soybeans':
        # soybean regex pattern starts with 'soybeans and products' and *normally* ends with 'sugar'
        start_pattern    = re.compile(r'U\.?\s*\.?S\.?\s*Soybeans\s+and\s+Products\s+Supply\s+and\s+Use', re.IGNORECASE)
        end_pattern      = re.compile(r'U\.?\s*\.?S\.?\s*Sugar\s+Supply\s+and\s+Use', re.IGNORECASE)
        alt_end_pattern  = re.compile(r'World\s+Soybean\s+Supply\s+and\s+Use\s+1/?', re.IGNORECASE)
        eof_pattern      = re.compile(r'End\s+of\s+File', re.IGNORECASE)

    start_match = start_pattern.search(text)
    if not start_match:
        return None

    line_start = text.rfind('\n', 0, start_match.start())
    if line_start == -1:                 # start marker is on the very first line
        line_start = 0
    else:
        line_start += 1                  # move past the newline character

    # Walk back the requested number of whole lines
    for _ in range(lines_before_start):
        prev_newline = text.rfind('\n', 0, line_start - 1)
        if prev_newline == -1:           # top of the file reached
            line_start = 0
            break
        line_start = prev_newline + 1    # start of the previous line

    if crop == 'soybean':
        end_match = end_pattern.search(text, pos=start_match.end())
        # the stats / reports crop order has changed over time, so let's look for a second end text option
        alt_end_match = end_pattern.search(text, pos=start_match.end())
        end_of_file_match = eof_pattern.search(text, pos=start_match.end())

        if end_match:
            return text[line_start:end_match.start()]
        elif alt_end_match:
            return text[line_start:alt_end_match.start()]
        elif end_of_file_match:
            return text[line_start:end_of_file_match.start()]
        else:
            return None

    end_match = end_pattern.search(text, pos=start_match.end())
    if end_match:
        return text[line_start:end_match.start()]
    else:
        return None

# find a txt link inside the <h2> “Releases”
def find_wasde_txt_link(soup: BeautifulSoup, month: int, year: int, crop: str) -> str | None:
    """
    Return the href of the first .txt link that appears under the
    <h2> heading whose text is “Releases”. This is to remove confusion over
    text files that you might get, since data under a <h2> tag of "Latest release"
    is present on ever page. The search respects the same priority order as before 
    (full date → two‑digit year → any .txt).
    """
    # Find the <h2> that says “Releases”
    releases_h2 = None
    for h2 in soup.find_all("h2"):
        if h2.get_text(strip=True).lower() == "releases":
            releases_h2 = h2
            break

    if not releases_h2:
        # no “Releases” heading – fall back to the whole page
        candidate_container = soup
    else:
        # build a temporary soup that contains only the siblings that
        # follow the “Releases” heading up to the next <h2>.
        following = []
        for sibling in releases_h2.next_siblings:
            if isinstance(sibling, str):
                continue
            if sibling.name == "h2":          # stop when the next heading starts
                break
            following.append(sibling)
        candidate_container = BeautifulSoup("".join(str(x) for x in following), "html.parser")

    # collect all .txt links inside the chosen container
    txt_links = [
        a["href"]
        for a in candidate_container.find_all("a", href=True)
        if a["href"].lower().endswith(".txt")
    ]

    # January and March 1995: return the second text file
    if year == 1995 and month in [1, 3]:
        return txt_links[1] if txt_links else None

    # September 2009: return the second text file
    if month == 9 and year == 2009:
        return txt_links[1] if txt_links else None

    # December 2010: text link is there, but doesn't contain the info we want
    if month == 12 and year == 2010:
        return None

    if crop == 'cotton':
        # # January 1995: return the second text file
        # if year == 1995 and month in [1]:
        #     return txt_links[1] if txt_links else None
        
        # January - April 1996: return the second text file
        if year == 1996 and month in [1, 2, 3, 4]:
            return txt_links[1] if txt_links else None

        # October 2008: return the third text file
        if month == 10 and year == 2008:
            return txt_links[2] if txt_links else None

    # wasde‑MM‑DD‑YYYY.txt, wasde‑MM‑DD‑YYYY_{crop}.txt, wasde‑MM‑DD‑YYYY_revision.txt, etc.
    primary_pat = re.compile(fr"^wasde-\d{2}-\d{2}-\d{4}(?:_{{crop}})?\.txt$", re.I)
    for href in txt_links:
        if primary_pat.match(os.path.basename(href)):
            return href

    # wasdeMMYY.txt or wasdeMMYYvN.txt
    secondary_pat = re.compile(r"^wasde\d{2}\d{2}(?:v\d+)?\.txt$", re.I)
    for href in txt_links:
        if secondary_pat.match(os.path.basename(href)):
            return href

    # any .txt fallback
    return txt_links[0] if txt_links else None


# validate that the chosen filename really belongs to the query month
def filename_matches_query(href: str, crop: str, query_year: int, query_month: int) -> bool:
    """
    Decide whether a discovered .txt (or .xls) link really belongs to the month
    we are querying.

    Accepted filename forms:
      wasde-MM-DD-YYYY.txt/.xls or wasde-MM-DD-YYYY_{crop}.txt
      wasdeMMYY.txt/.xls or wasdeMMYYvN.txt/.xls
      latest.txt/latest.xls)
      Any other file that explicitly contains the four-digit year we are looking for
    """
    from os.path import basename

    fname = basename(href).lower()

    # wasde‑MM‑DD‑YYYY.txt/.xls or wasde‑MM‑DD‑YYYY_{crop}.txt
    m = re.fullmatch(fr"wasde-\d{2}-\d{2}-\d{4}(?:_{{crop}})?\.txt | wasde-(\d{2})-\d{2}-(\d{4})\.xls", fname)
    if m:
        month = int(m.group(1) or m.group(3))
        year  = int(m.group(2) or m.group(4))
        return month == query_month and year == query_year

    # wasdeMMYY.txt/.xls   (two‑digit year)
    # wasdeMMYYvN.txt/.xls (version suffix)
    m = re.fullmatch(
        r"wasde(\d{2})(\d{2})(?:v\d+)?\.txt|wasde(\d{2})(\d{2})(?:v\d+)?\.xls",
        fname,
    )
    if m:
        month = int(m.group(1) or m.group(3))
        year  = 2000 + int(m.group(2) or m.group(4))   # assumes 2000‑2099
        return month == query_month and year == query_year

    # generic filenames that i have found so far: latest.txt/.xls
    # note: sometimes there are files called readme.txt, which are not what we want
    if fname in ('latest.txt', 'latest.xls'):
        return True

    # any other file that contains the full year (e.g. “something2021.txt”)
    if str(query_year) in fname:
        return True

    # nothing matched → not a suitable file for this month
    return False


# find an XLS link using the same priority rules
def find_wasde_xls_link(soup: BeautifulSoup, month: int, year: int) -> str | None:
    """
    Mirrors `find_wasde_txt_link` but looks for *.xls* files.
    Returns the first href that matches the priority order:
        wasde-MM-DD-YYYY.xls
        wasdeMMYY.xls or wasdeMMYYvN.xls
        any other .xls fallback
    
    This search is also limited to the <h2> “Releases” section.
    """
    # locate the same “Releases” container we used for TXT files
    releases_h2 = None
    for h2 in soup.find_all("h2"):
        if h2.get_text(strip=True).lower() == "releases":
            releases_h2 = h2
            break

    if not releases_h2:
        container = soup
    else:
        following = []
        for sibling in releases_h2.next_siblings:
            if isinstance(sibling, str):
                continue
            if sibling.name == "h2":
                break
            following.append(sibling)
        container = BeautifulSoup("".join(str(x) for x in following), "html.parser")

    # gather all .xls links inside the container
    xls_links = [
        a["href"]
        for a in container.find_all("a", href=True)
        if a["href"].lower().endswith(".xls")
    ]

    # wasde‑MM‑DD‑YYYY.xls
    primary_pat = re.compile(r"^wasde-\d{2}-\d{2}-\d{4}\.xls$", re.I)
    for href in xls_links:
        if primary_pat.match(os.path.basename(href)):
            return href

    # wasdeMMYY.xls  or wasdeMMYYvN.xls
    secondary_pat = re.compile(r"^wasde\d{2}\d{2}(?:v\d+)?\.xls$", re.I)
    for href in xls_links:
        if secondary_pat.match(os.path.basename(href)):
            return href

    # any .xls fallback
    return xls_links[0] if xls_links else None


def excel_col_letter(idx: int) -> str:
    """
    Convert a zero‑based column index to an Excel column letter (A, B, …).
    """
    letters = ""
    idx += 1
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    
    return letters

def search_sheet_for_pattern(crop: str, df: pd.DataFrame) -> list[dict]:
    """
    Scan a DataFrame (one worksheet) for cells that match `pattern`.
    Returns a list of dicts with row, column letter, and the cell's string value.
    """
    if crop == 'corn':
        # corn regex pattern starts with 'feed grain and corn' and *normally* ends with 'sorghum, barley(,) and oats'
        pattern = re.compile(r'U\.?\s*\.?S\.?\s*Feed\s+Grain\s+and\s+Corn\s+Supply\s+and\s+Use\s+1', re.IGNORECASE)
    elif crop == 'cotton':
        # cotton regex pattern starts with 'cotton' and *normally* ends with 'world wheat'
        pattern = re.compile(r'U\.?\s*\.?S\.?\s*Cotton\s+Supply\s+and\s+Use\s+1', re.IGNORECASE)
    else: # crop == 'soybeans':
        # soybean regex pattern starts with 'soybeans and products' and *normally* ends with 'sugar'
        pattern = re.compile(fr'U\.?\s*\.?S\.?\s*Soybeans\s+and\s+Products\s+Supply\s+and\s+Use', re.IGNORECASE)

    matches = []

    # normalise a Series to a one‑column DataFrame (handles single‑column sheets)
    if isinstance(df, pd.Series):
        df = df.to_frame(name="__single_column__")

    arr = df.values
    n_rows, n_cols = arr.shape

    for r in range(n_rows):
        for c in range(n_cols):
            cell_str = str(arr[r, c])          # safe conversion (NaN → 'nan')
            if pattern.search(cell_str):
                matches.append({
                    "row": r + 1,              # 1‑based row number (Excel style)
                    "col": excel_col_letter(c),
                    "value": cell_str,
                })
    
    return matches


def scrape_wasde_data():
    """
    Main webscraping script for data coming from the USDA's World Agricultural Supply and Demand Estimates site.

    Notes:
    All crops:
        - Data for January and March 1995: look for second text file

        - Data for September 2009 contains two text files and we need the second one.

        - Data for December 2010: ignore the text file, look for the XLS file instead.

    - Corn:
        - Data after September 2016: 

    - Cotton: 
        - Data January 1995 - April 1996: our regex pattern does not currently work for these files. We match on the first pattern 
        (e.g., 'U.S. Cotton Supply and Use'), but not the second pattern ('U.S. Wheat Supply and Use')
        because it does not exist (the text file ends). We would need to alter the extract_cotton_section() to 
        work until the end of the file specifically for data for that year.

        - Data for January - April 1996 contains two text files and we need the second one. See note above about regex patterns.

        - Data for October 2008 contains three text files: we need the third one.

        - Data from October 2010 through September 2016 is only available as XLS or PDF files.

        - Data for December 2024, April 2025, and May 2025 ends in {filanme}v2.txt

        - Data for October 2013, January 2019, and October 2025 is not available.

    - Soybeans:
        - Data for October 2008: first end regex search pattern fails because we reach the end of the file,
                                 so the script looks for alternate regex patterns

    """
    def valid_crop(crop: str) -> str:
        """
        Validate that the `crop` argument is one of the crops this script currently supports.
        """
        try:
            crop = str(crop)
        except:
            raise argparse.ArgumentTypeError('Crop values must be of type `str`.')

        valid_crops = ['corn', 'cotton', 'soybean']
        if crop not in valid_crops:
            raise argparse.ArgumentTypeError(f"Crop `{crop}` is not one of: ['corn', 'cotton', 'soybeans'].")

        return crop


    def valid_month(month: str) -> int:
        """
        Assert that input argument `month` is of type int and is in the range [1, 12].
        """
        try:
            month = int(month)
        except:
            raise argparse.ArgumentTypeError('Month values must be of type `int`.')

        valid_months = list(range(1, 13, 1))
        if month not in valid_months:
            raise argparse.ArgumentTypeError(f'Month values must be between [1, 12].')

        return month


    def valid_year(year: str) -> None:
        """
        Assert that input argument `year` is of type int and is in the range [1995, 2026].
        """
        try:
            year = int(year)
        except:
            raise argparse.ArgumentTypeError('Year values must be of type `int`.')

        valid_years = list(range(1995, 2027, 1))
        if year not in valid_years:
            raise argparse.ArgumentTypeError(f'Year values must be between [{valid_years[0]}, {valid_years[-1]}].')

        return year
        

    parser = argparse.ArgumentParser(description='Web scraping script for USDA WASDE crop statistics.')
    
    parser.add_argument(
        '-c', 
        '--crops', 
        type=lambda x: valid_crop(x),
        nargs='+',
        required=True, 
        choices=['corn', 'cotton', 'soybean'],
        help='Crop name(s).'
    )

    parser.add_argument(
        '-sy',
        '--start-year',
        type=valid_year,
        default=1995,
        help='Year to begin web scraping. Min supported year is `1995`.')

    parser.add_argument(
        '-sm',
        '--start-month',
        type=valid_month,
        default=1,
        help='Month to begin web scraping. Min month is `1`.')

    parser.add_argument(
        '-ey',
        '--end-year',
        type=valid_year,
        default=datetime.today().year,
        help='Year to end web scraping. Max supported year is `2026`.')

    parser.add_argument(
        '-em',
        '--end-month',
        type=valid_month,
        default=12,
        help='Month to end web scraping. Max month is `12`.')
    
    parser.add_argument(
        '-s',
        '--show-output',
        type=bool,
        default=False,
        choices=[True, False],
        help='If `False`, include all print output, not for dates where \
            the script fails or there is known missing data.')

    args = parser.parse_args()
    CROPS = args.crops
    START_YEAR = args.start_year
    START_MONTH = args.start_month
    END_YEAR = args.end_year
    END_MONTH = args.end_month
    SHOW_OUTPUT = args.show_output

    # check to make sure end year >= start year, end month >= start month
    if END_YEAR < START_YEAR:
        raise ValueError('Please enter an end year >= to the start year.')

    # we only need to check if end month >= start month when downloading data for one calendar year
    if (END_YEAR == START_YEAR) and (END_MONTH < START_MONTH):
        raise ValueError('Please enter an end month >= to the start month.')

    BASE_URL = ('https://esmis.nal.usda.gov/publication/world-agricultural-supply-and-demand-estimates')
    SCRIPT_DIR = Path(__file__).resolve().parent             # ./src
    PROJECT_ROOT = SCRIPT_DIR.parent                         # .. project root

    for crop in CROPS:             
        OUTPUT_DIR = PROJECT_ROOT / 'data' / 'raw' / f'{crop}'   # e.g., ../data/raw/soy/
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        session = requests.Session()

        print()
        print(f'Downloading data for {crop} from {START_YEAR}-{str(START_MONTH).zfill(2)} til {END_YEAR}-{str(END_MONTH).zfill(2)}')

        dates_to_download = list(month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH))
        for year, month in tqdm(dates_to_download, position=0, leave=True):
            # note, this is for all crops
            no_data_list = ['10-2013', '01-2019', '10-2025']
            
            # same here: the text files do not exist for any crop, so look for XLS files instead
            no_text_list = pd.date_range(start='2010-10-01', end='2016-09-01', freq='MS').strftime('%m-%Y').tolist()

            if f"{month:02d}-{year}" in no_data_list:
                # for the month, we can add a 0 to the int version to make the years the same
                tqdm.write(f"   ⚠️  Ignoring data for {month:02d}-{year} because it does not exist.")
                tqdm.write('')
                continue

            query_url = build_query_url(BASE_URL, year, month)
            if SHOW_OUTPUT:
                tqdm.write(f"   🔎 {year:04d}-{month:02d} → {query_url}")

            # retrieve the HTML page for the month
            try:
                page_resp = session.get(query_url, timeout=30)
                page_resp.raise_for_status()
            except Exception as exc:
                tqdm.write(f"   ❌ Could not retrieve page: {exc}")
                continue

            soup = BeautifulSoup(page_resp.text, "html.parser")

            # try to find a matching TEXT file
            txt_href = find_wasde_txt_link(soup, month, year, crop)

            if txt_href and filename_matches_query(txt_href, crop, year, month):
                # ------------ TEXT path ----------------------------
                txt_url = resolve_absolute(BASE_URL, txt_href)
                if SHOW_OUTPUT:
                    tqdm.write(f"   📥 Downloading: {txt_url}")

                try:
                    raw_txt = download_text(txt_url)
                except Exception as exc:
                    tqdm.write(f"   ❌ Failed to download .txt: {exc}")
                    tqdm.write('')
                    continue

                crop_block = extract_crop_section(crop, raw_txt)

                if not crop_block:
                    tqdm.write(f"   ⚠️  Markers not found for {year}-{str(month).zfill(2)} - skipping this TXT file.")
                    tqdm.write('')
                    continue

                out_path = OUTPUT_DIR / f"{crop}_{year:04d}_{month:02d}.txt"
                out_path.write_text(crop_block, encoding="utf-8")
                
                if SHOW_OUTPUT:
                    tqdm.write(f"   ✅ Saved → {out_path}")
                    tqdm.write('')
                continue   # we’re done for this month – go to next iteration

            # no suitable TXT → try XLS fallback
            xls_href = find_wasde_xls_link(soup, month, year)

            if not xls_href or not filename_matches_query(xls_href, crop, year, month):
                # Neither TXT nor XLS matched – report and move on
                tqdm.write(f"   ⚠️  No matching .txt or .xls file found for this {year}-{str(month).zfill(2)}.")
                tqdm.write('')
                continue

            # ------------ XLS path ----------------------------------
            xls_url = resolve_absolute(BASE_URL, xls_href)
            if SHOW_OUTPUT:
                tqdm.write(f"   📥 XLS fallback found: {xls_url}")

            try:
                xls_bytes = download_binary(xls_url)
            except Exception as exc:
                tqdm.write(f"   ❌ Failed to download .xls: {exc}")
                tqdm.write('')
                continue

            # load the workbook (all sheets) into a dict of DataFrames
            try:
                sheets_dict = pd.read_excel(io.BytesIO(xls_bytes), sheet_name=None)
            except Exception as exc:
                tqdm.write(f"   ❌ Could not parse the XLS workbook: {exc}")
                tqdm.write('')
                continue

            # search each sheet for the soy- or corn‑supply regex and keep only those that contain at least one match
            matching_sheets = {}

            for sheet_name, df in sheets_dict.items():
                matches = search_sheet_for_pattern(crop, df)
                if matches: # if this sheet has a hit, keep it
                    matching_sheets[sheet_name] = df

            if not matching_sheets:
                tqdm.write('   ⚠️  No worksheet contained the search pattern - skipping XLS.')
                tqdm.write('')
                continue

            # write ONLY the matching worksheets to a new XLS file
            out_path = OUTPUT_DIR / f"{crop}_{year:04d}_{month:02d}.xls"
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                for sheet_name, df in matching_sheets.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            if SHOW_OUTPUT:
                tqdm.write(f"   ✅ Saved matching sheet(s) → {out_path}")
                tqdm.write('')
        
        print()
        print(f'🎉 Finished! All extracted files for {crop} are in: {OUTPUT_DIR.resolve()}')
        print('')


if __name__ == "__main__":
    try:
        scrape_wasde_data()
    except KeyboardInterrupt:
        print()
        sys.exit('Interrupted by user.')