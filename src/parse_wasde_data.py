#!/usr/bin/env python3
"""
We would like to have monthly data from the USDA WASDE site that goes as far back as possible.
Unfortunately, before 1995, the data for some crops is in PDF form instead of TXT (and later, XLS) files.
The data format also changes quite a bit over the years for each individual crop, both in naming conventions,
where data is placed in a report (which interferes with regex search patterns), and more.
This script parses each crop's TXT and XLS files, combines them into a single Pandas DataFrame, then saves the
combined data locally.
"""

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm


def convert_and_pad_list(input_list: list) -> list:
    """
    Given an input list, replace all string values with float equivalents.
    Then, make sure the list has 4 elements. If not, insert a NaN value at index 3.
    """
    input_list = [float(val) if val != 'na' else np.nan for val in input_list]
    if len(input_list) == 3:
        input_list.insert(2, np.nan)

    return input_list


def create_wasde_df(dates: list, plines: list, hlines: list, ylines: list) -> pd.DataFrame:
    """
    Given an input list for dates, planted and harvested area data, and yield data,
    return a Pandas DataFrame representing scraped monthly data from the USDA WASDE site.

    This method works to build the DataFrame for manually created data and parsed TXT and XLS files.
    """
    df = pd.DataFrame(
        {
            # duplicate time column for a datetime index that will be dropped later
            'dates': dates,
            'time': dates,
            'year_minus_2_planted_area': [num[0] for num in plines],
            'year_minus_1_planted_area': [num[1] for num in plines],
            'current_year_last_month_planted_area': [num[2] for num in plines],
            'current_year_current_month_planted_area': [num[3] for num in plines],
            'year_minus_2_harvested_area': [num[0] for num in hlines],
            'year_minus_1_harvested_area': [num[1] for num in hlines],
            'current_year_last_month_harvested_area': [num[2] for num in hlines],
            'current_year_current_month_harvested_area': [num[3] for num in hlines],
            'year_minus_2_yield': [num[0] for num in ylines],
            'year_minus_1_yield': [num[1] for num in ylines],
            'current_year_last_month_yield': [num[2] for num in ylines],
            'current_year_current_month_yield': [num[3] for num in ylines],
        }
    )

    df = df.set_index('dates')

    return df


def pad_df_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given an input Pandas DataFrame, make sure that we have no 'missing' rows,
    i.e., add empty rows representing months where we don't have data.
    """
    # add empty rows for missing dates / data
    full_date_list = pd.date_range(start=df.index.min(), end=df.index.max(), freq='MS')
    df = df.reindex(full_date_list)
    df['time'] = df.index

    # now let's format the time column as a string
    df = df.reset_index(drop=True)
    df['time'] = df.time.dt.strftime('%Y-%m-%d')

    return df


def add_manual_data(crop: str) -> pd.DataFrame:
    """
    We need to add in some data manually, primarily the May data for 1991 - 1995.
    The official data the USDA uses in reports and figures comes out in May, so instead of
    adding in monthly data for these dates, we are only going to focus on annual data.
    """
    # https://esmis.nal.usda.gov/publication/world-agricultural-supply-and-demand-estimates?date=YYYY-05
    # where YYYY could be: [1991, 1995]

    if crop == 'corn':
        dates = [
            pd.to_datetime(date)
            for date in ['1991-05-01', '1992-05-01', '1993-05-01', '1994-05-01']
        ]

        pa_lines = [
            [72.2, 74.2, np.nan, np.nan],
            [74.2, 76.0, np.nan, np.nan],
            [76.0, 79.3, np.nan, 76.5],
            [79.3, 73.3, np.nan, 78.6],
        ]

        ha_lines = [
            [64.7, 67.0, np.nan, np.nan],
            [67.0, 68.8, np.nan, np.nan],
            [68.8, 72.1, np.nan, 69.3],
            [72.2, 63.0, np.nan, 71.5],
        ]

        y_lines = [
            [116.3, 118.5, np.nan, np.nan],
            [118.5, 108.6, np.nan, np.nan],
            [108.6, 131.4, np.nan, 122.7],
            [131.4, 100.7, np.nan, 122.1],
        ]

    elif crop == 'cotton':
        dates = [
            pd.to_datetime(date)
            for date in ['1991-05-01', '1992-05-01', '1993-05-01', '1994-05-01', '1995-05-01']
        ]

        pa_lines = [
            [10.59, 12.35, np.nan, np.nan],
            [12.35, 14.05, np.nan, np.nan],
            [14.05, 13.24, np.nan, 13.43],
            [13.24, 13.44, np.nan, 13.84],
            [13.44, 13.73, np.nan, 16.20],
        ]

        ha_lines = [
            [9.54, 11.73, np.nan, np.nan],
            [11.73, 12.96, np.nan, np.nan],
            [12.96, 11.14, np.nan, 12.36],
            [11.14, 12.79, np.nan, 12.80],
            [12.78, 13.33, np.nan, 15.15],
        ]

        y_lines = [
            [614, 634, np.nan, np.nan],
            [634, 652, np.nan, np.nan],
            [652, 699, np.nan, 680],
            [699, 606, np.nan, 665],
            [606, 708, np.nan, 665],
        ]

    else:  # crop == 'soybeans':
        dates = [
            pd.to_datetime(date)
            for date in ['1991-05-01', '1992-05-01', '1993-05-01', '1994-05-01']
        ]

        pa_lines = [
            [60.8, 57.8, np.nan, np.nan],
            [57.8, 59.1, np.nan, np.nan],
            [59.2, 59.3, np.nan, 59.3],
            [59.1, 59.4, np.nan, 61.1],
        ]

        ha_lines = [
            [59.5, 56.5, np.nan, np.nan],
            [56.5, 58.0, np.nan, np.nan],
            [58.0, 58.4, np.nan, 59.3],
            [58.2, 56.4, np.nan, 60.0],
        ]

        y_lines = [
            [32.3, 34.0, np.nan, np.nan],
            [34.0, 34.3, np.nan, np.nan],
            [34.2, 37.6, np.nan, 35.1],
            [37.6, 32.0, np.nan, 35.0],
        ]

    df = create_wasde_df(dates, pa_lines, ha_lines, y_lines)

    return df


def parse_txt_files(filenames: list, crop: str) -> pd.DataFrame:
    """
    Parse TXT files scraped from the USDA WASDE site given a list of filenames and a crop name.
    """
    dates = []
    pa_lines = []
    ha_lines = []
    y_lines = []

    for filename in filenames:
        year, month = filename[-11:-4].split('_')
        date = f"{year}-{month}-01"
        dates.append(pd.to_datetime(date))

        with open(filename) as f:
            lines = [
                line.lower().replace('\n', '').replace('*', '').replace(':', '').lstrip().rstrip()
                for line in f
            ]

            if crop == 'corn':
                matching_index = lines.index('corn')
                planted_index = (
                    matching_index + 2 if pd.to_datetime(date).year < 2016 else matching_index + 1
                )
                harvested_index = (
                    matching_index + 3 if pd.to_datetime(date).year < 2016 else matching_index + 2
                )
                # works for both pre- and post-2016 data
                yield_index = matching_index + 5

                matching_lines = lines[planted_index]
                pa_line = matching_lines.replace('area', '').replace('planted', '').split()
                pa_line = convert_and_pad_list(pa_line)
                pa_lines.append(pa_line)

                matching_lines = lines[harvested_index]
                ha_line = matching_lines.replace('area', '').replace('harvested', '').split()
                ha_line = convert_and_pad_list(ha_line)
                ha_lines.append(ha_line)

                matching_lines = lines[yield_index]
                # the 2016 data format change includes the word 'acre' in the line we are looking for
                # we can apply the .replace() method to all lines because if the term isn't found, we don't replace anything
                y_line = (
                    matching_lines.replace('yield per harvested', '').replace('acre', '').split()
                )
                y_line = convert_and_pad_list(y_line)
                y_lines.append(y_line)

            elif crop == 'cotton':
                matching_lines = [line for line in lines if 'planted' in line]
                pa_line = matching_lines[0].replace('planted', '').split()
                pa_line = convert_and_pad_list(pa_line)
                pa_lines.append(pa_line)

                matching_lines = [line for line in lines if 'harvested' in line]
                ha_line = matching_lines[0].replace('harvested', '').split()
                ha_line = convert_and_pad_list(ha_line)
                ha_lines.append(ha_line)

                # the label for the yield row changes in June 1998
                if pd.to_datetime(f'{year}-{month}-01') < pd.to_datetime('1998-06-01'):
                    matching_lines = [line for line in lines if 'yield per harv. acre' in line]
                    y_line = matching_lines[0].replace('yield per harv. acre', '').split()
                else:
                    pattern = re.compile(r'^.*\bacre\b.*$', re.MULTILINE | re.IGNORECASE)
                    matching_lines = [line for line in lines if pattern.search(line)]
                    y_line = matching_lines[0].replace('acre', '').split()

                y_line = convert_and_pad_list(y_line)
                y_lines.append(y_line)

            else:  # crop == 'soybeans'
                matching_lines = [line for line in lines if 'planted' in line]
                pa_line = matching_lines[0].replace('area', '').replace('planted', '').split()
                pa_line = convert_and_pad_list(pa_line)
                pa_lines.append(pa_line)

                matching_lines = [line for line in lines if 'harvested' in line]
                ha_line = matching_lines[0].replace('area', '').replace('harvested', '').split()
                ha_line = convert_and_pad_list(ha_line)
                ha_lines.append(ha_line)

                # the label for the yield row changes in June 1998
                if pd.to_datetime(f'{year}-{month}-01') < pd.to_datetime('1996-05-01'):
                    matching_lines = [line for line in lines if 'unit' in line]
                    y_line = matching_lines[0].replace('unit', '').split()
                elif pd.to_datetime(f'{year}-{month}-01') == pd.to_datetime('1996-05-01'):
                    matching_lines = [line for line in lines if 'yield per harv. acre' in line]
                    y_line = matching_lines[0].replace('yield per harv. acre', '').split()
                elif pd.to_datetime(f'{year}-{month}-01') > pd.to_datetime(
                    '1996-05-01'
                ) and pd.to_datetime(f'{year}-{month}-01') < pd.to_datetime('2016-10-01'):
                    pattern = re.compile(r'^.*\bacre\b.*$', re.MULTILINE | re.IGNORECASE)
                    matching_lines = [line for line in lines if pattern.search(line)]
                    y_line = matching_lines[0].replace('acre', '').split()
                else:  # date > 2016-10-01
                    matching_lines = [line for line in lines if 'yield per harvested acre' in line]
                    y_line = matching_lines[0].replace('yield per harvested acre', '').split()

                y_line = convert_and_pad_list(y_line)
                y_lines.append(y_line)

    df = create_wasde_df(dates, pa_lines, ha_lines, y_lines)

    return df


def parse_xls_files(filenames: list, crop: str) -> pd.DataFrame:
    """
    Parse XLS files scraped from the USDA WASDE site given a list of filenames and a crop name.
    """
    if crop == 'corn':
        XLS_SEARCH_TERM = 'Feed Grain and Corn Supply and Use'
    elif crop == 'cotton':
        XLS_SEARCH_TERM = 'Cotton Supply and Use'
    else:  # crop == 'soybeans'
        XLS_SEARCH_TERM = 'Soybeans and Products Supply and Use'

    dates = []
    pa_lines = []
    ha_lines = []
    y_lines = []

    for filename in filenames:
        year, month = filename[-11:-4].split('_')
        date = f"{year}-{month}-01"
        dates.append(pd.to_datetime(date))

        # test case files: cotton_2016_08 has a different format than cotton_2010_10
        df = pd.read_excel(filename)
        # drop filler columns and rows
        df = df[~df.apply(lambda row: row.astype(str).str.contains(XLS_SEARCH_TERM).any(), axis=1)]
        df = df[~df.apply(lambda row: row.astype(str).str.contains('WASDE').any(), axis=1)]
        df = df.dropna(how='all', axis=1).dropna(how='all', axis=0)

        if crop == 'corn':
            df = df.reset_index(drop=True)

            # we want to ignore everything between the lines `FEED GRAINS` and `CORN`
            # since we are only interested in planted / harvested acres and yield info for corn
            start_index = df.iloc[:, 0].values.tolist().index(f'{crop.upper()}')
            df = df.iloc[start_index:, :]

            df.columns = ['label', 'year_minus_2', 'year_minus_1', 'last_month', 'current_month']
            df['label'] = df.label.str.lower().str.strip()
            df = df.query(
                " label == 'area planted' or label == 'area harvested' or label == 'yield per harvested acre' "
            )

            # remove all asterisks
            for column in df.columns:
                df[column] = df[column].astype(str).str.replace('*', '')

            pa_line = df.query(" label == 'area planted' ").iloc[:, 1:].values[0].tolist()
            ha_line = df.query(" label == 'area harvested' ").iloc[:, 1:].values[0].tolist()
            y_line = (
                df.query(" label == 'yield per harvested acre' ").iloc[:, 1:].values[0].tolist()
            )

            pa_line = convert_and_pad_list(pa_line)
            ha_line = convert_and_pad_list(ha_line)
            y_line = convert_and_pad_list(y_line)

            pa_lines.append(pa_line)
            ha_lines.append(ha_line)
            y_lines.append(y_line)

        elif crop == 'cotton':
            df.columns = ['label', 'year_minus_2', 'year_minus_1', 'last_month', 'current_month']

            df['label'] = df.label.str.lower().str.strip()
            df = df.query(
                " label == 'planted' or label == 'harvested' or label == 'yield per harvested acre' "
            )

            # remove all asterisks
            for column in df.columns:
                df[column] = df[column].astype(str).str.replace('*', '')

            pa_line = df.query(" label == 'planted' ").iloc[:, 1:].values[0].tolist()
            ha_line = df.query(" label == 'harvested' ").iloc[:, 1:].values[0].tolist()
            y_line = (
                df.query(" label == 'yield per harvested acre' ").iloc[:, 1:].values[0].tolist()
            )

            pa_line = convert_and_pad_list(pa_line)
            ha_line = convert_and_pad_list(ha_line)
            y_line = convert_and_pad_list(y_line)

            pa_lines.append(pa_line)
            ha_lines.append(ha_line)
            y_lines.append(y_line)

        else:  # crop == 'soybeans'
            if pd.to_datetime(date) < pd.to_datetime('2015-07-01'):
                df.columns = [
                    'label',
                    'date',
                    'year_minus_2',
                    'year_minus_1',
                    'last_month',
                    'current_month',
                ]
            else:
                df.columns = [
                    'label',
                    'year_minus_2',
                    'year_minus_1',
                    'last_month',
                    'current_month',
                ]

            df['label'] = df.label.str.lower().str.strip()
            df = df.query(
                " label == 'area planted' or label == 'area harvested' or label == 'yield per harvested acre' "
            )

            # remove all asterisks
            for column in df.columns:
                df[column] = df[column].astype(str).str.replace('*', '')

            # the soybean data has an extra 'date' column for some month-years, so we need to shift up a column in the values that we take
            if pd.to_datetime(date) < pd.to_datetime('2015-07-01'):
                pa_line = df.query(" label == 'area planted' ").iloc[:, 2:].values[0].tolist()
                ha_line = df.query(" label == 'area harvested' ").iloc[:, 2:].values[0].tolist()
                y_line = (
                    df.query(" label == 'yield per harvested acre' ").iloc[:, 2:].values[0].tolist()
                )
            else:
                pa_line = df.query(" label == 'area planted' ").iloc[:, 1:].values[0].tolist()
                ha_line = df.query(" label == 'area harvested' ").iloc[:, 1:].values[0].tolist()
                y_line = (
                    df.query(" label == 'yield per harvested acre' ").iloc[:, 1:].values[0].tolist()
                )

            pa_line = convert_and_pad_list(pa_line)
            ha_line = convert_and_pad_list(ha_line)
            y_line = convert_and_pad_list(y_line)

            pa_lines.append(pa_line)
            ha_lines.append(ha_line)
            y_lines.append(y_line)

    df = create_wasde_df(dates, pa_lines, ha_lines, y_lines)

    return df


def parse_wasde_data():
    """
    Combine manually added USDA WASDE data to parsed TXT and XLS file data. Then, save the combined output locally.

    Creating and parsing data (manual add, parsing TXT / XLS files) are done in separate functions. Should you want to
    add more data manually or should the data format for future TXT files change again, it should be relatively easy
    to update individual function calls, but the bulk of the parse_wasde_data() function should not need to change.
    """

    def valid_crop(crop: str) -> str:
        """
        Validate that the `crop` argument is one of the crops this script currently supports.
        """
        try:
            crop = str(crop)
        except ValueError:
            raise argparse.ArgumentTypeError('Crop values must be of type `str`.')

        valid_crops = ['corn', 'cotton', 'soybeans']
        if crop not in valid_crops:
            raise argparse.ArgumentTypeError(
                f"Crop `{crop}` is not one of: ['corn', 'cotton', 'soybeans']."
            )

        return crop

    parser = argparse.ArgumentParser(
        description='Web scraping script for USDA WASDE crop statistics.'
    )

    parser.add_argument(
        '-c',
        '--crops',
        type=lambda x: valid_crop(x),
        nargs='+',
        choices=['corn', 'cotton', 'soybeans'],
        default=['corn', 'cotton', 'soybeans'],
        help='Crop name(s).',
    )

    args = parser.parse_args()
    CROPS = args.crops

    SCRIPT_DIR = Path(__file__).resolve().parent  # ./src
    PROJECT_ROOT = SCRIPT_DIR.resolve().parent  # .. project root

    print()
    for crop in tqdm(CROPS, position=0, leave=True):
        tqdm.write(f"Processing data for {crop}")

        INPUT_DIR = PROJECT_ROOT / 'data' / 'raw' / f'{crop}'  # e.g., ../data/raw/cotton/
        OUTPUT_DIR = (
            PROJECT_ROOT / 'data' / 'processed' / f'{crop}'
        )  # e.g., ../data/processed/cotton/

        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # start building the combined data
        df_manual = add_manual_data(crop)

        txt_filenames = [str(filename) for filename in sorted(list(INPUT_DIR.glob('*.txt')))]
        df_txt = parse_txt_files(txt_filenames, crop)

        xls_filenames = [str(filename) for filename in sorted(list(INPUT_DIR.glob('*.xls')))]
        df_xls = parse_xls_files(xls_filenames, crop)

        # df_txt already has empty rows for the dates where there are xls files
        # so we want to infill those while keeping df_txt the same
        df = pd.concat([df_manual, df_txt, df_xls]).sort_values(by='time')
        df = pad_df_dates(df)

        # check for missing dates
        data_cols = df.columns.values[1:].tolist()
        all_nan_mask = df[data_cols].isna().all(axis=1)
        found_missing_dates = df.loc[all_nan_mask, 'time'].tolist()

        known_missing_dates = ['2013-10-01', '2019-01-01', '2025-10-01']
        # if this test passes, then we have data for all but when we know it is missing
        # because we only imported data for may in 1992-95, we can ignore those dates
        assert found_missing_dates[-3:] == known_missing_dates

        # save combined data locally
        df.to_excel(f'{OUTPUT_DIR}/{crop}_1991_2026.xlsx', index=False)

    print()
    print(f'🎉 Finished! All processed files are in: {PROJECT_ROOT / "data" / "processed"}')
    print()


if __name__ == "__main__":
    try:
        parse_wasde_data()
    except KeyboardInterrupt:
        print()
        sys.exit('Interrupted by user.')
