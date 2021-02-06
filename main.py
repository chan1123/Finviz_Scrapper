import pandas as pd
import numpy as np
import datetime
import os
import time
import requests
from bs4 import BeautifulSoup

def return_filename():
    """
    Return filename of today
    :return: string
    """

    date_time = datetime.datetime.now().strftime("%Y%m%d")
    filename = './Finviz/' + date_time + '_total' + '.csv'
    return filename

def return_single_table_from_finviz(table_pos, base_url, parameters, total_stocks=False):
    """
    Submit requests to finviz and scrap only one stock table
    """
    my_headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url=base_url,
                     params=parameters,
                     headers=my_headers)

    html_contents = r.text
    html_soup = BeautifulSoup(html_contents, 'html.parser')

    dfs = pd.read_html(html_contents)

    if True == total_stocks:
        return dfs[table_pos], int(dfs[table_pos - 1].iloc[0, 0].split(' ')[1])
    return dfs[table_pos]


def convert_datatypes_overview(df):
    """
    Return the cleaned dataframe
    """

    # Reduce memory usage by setting object as category
    category_list = ['Sector', 'Industry', 'Country']

    df[category_list] = df[category_list].astype('category')

    df = df.loc[:, ['Company', 'Sector', 'Industry', 'Country']]
    df.columns.name = 'Overview'
    return df


def scrap_all_stocks():
    """
    Scrap all stocks
    Only option is technical and overview
    See other options in Scrap Finviz file
    """
    base_url = 'https://finviz.com/screener.ashx'
    parameters = {'f': 'ind_stocksonly', 'v': 111}
    position = 14

    # Check if the program has been run today
    filename = return_filename()
    if os.path.isfile(filename):
        return pd.read_csv(filename, index_col='Ticker')

    trial = return_single_table_from_finviz(table_pos=position,
                                            base_url=base_url,
                                            parameters=parameters,
                                            total_stocks=True)
    total = trial[1]
    results = trial[0]
    print(total)

    total = 60
    for _ in list(range(21, total, 20)):
        parameters['r'] = _

        new_frame = return_single_table_from_finviz(table_pos=position,
                                                    base_url=base_url,
                                                    parameters=parameters)

        new_frame.drop(new_frame.index[0], inplace=True)

        results = results.append(new_frame)

        time.sleep(3)
        print(_, 'is done')

    results.columns = results.iloc[0]
    results.drop(index=[0], inplace=True)
    results.set_index('Ticker', inplace=True)
    del results['No.']

    # Saving the stocks to folder

    converted_result = convert_datatypes_overview(results)
    converted_result.to_csv(filename)

    return converted_result

df = print(scrap_all_stocks())