# system
import time
# import os
# CURR_PATH = os.path.abspath('')
# ROOT_PATH = os.path.abspath(os.path.join(CURR_PATH, os.pardir))

# general
from dotenv import dotenv_values
import pandas as pd
import sqlite3

# third party
from utils.rutracker import Rutracker


def search_rt(sort_by:str):
    config = dotenv_values(f".env")
    rt = Rutracker(
        login=config['rutracker_login'],
        password=config['rutracker_password'],
        tracker_url='https://rutracker.org/',
        logging_mode='console'
    )
    return pd.DataFrame(
        [
            {
                'section': f,
                'title': t,
                'id': tid,
                'size_bytes': s,
                'number_of_seeds': se,
                'number_of_leeches': le,
                'number_of_downloads': do,
                'date_added': da
            }
            for f, t, tid, s, se, le, do, da in rt.search(sort_by=sort_by)
        ]
    )


def split_to_dim_and_fact(df:pd.DataFrame):
    tt_dim = df.copy()
    tt_fact = df.copy()
    tt_dim.drop(columns=['number_of_seeds', 'number_of_leeches', 'number_of_downloads'], inplace=True)
    tt_fact.drop(columns=['section', 'title', 'size_bytes', 'date_added'], inplace=True)
    return tt_dim, tt_fact


def append_to_dim(df:pd.DataFrame):
    df.drop_duplicates(subset=['id'], keep='last', inplace=True)
    df.to_sql(
        name='torrent_dim',
        con=sqlite3.connect("./data/local.db"),
        if_exists='append'
    )


def append_to_fact(df:pd.DataFrame):
    df.drop_duplicates(subset=['id'], keep='last', inplace=True)
    df['fact_timest'] = int(time.time())
    df.to_sql(
        name='torrent_fact',
        con=sqlite3.connect("./data/local.db"),
        if_exists='append'
    )


if __name__ == '__main__':

    dfs = [
        split_to_dim_and_fact(search_rt(sort_by=mode))
        for mode in ['number_of_seeds', 'number_of_leeches', 'date_added']
    ]

    tt_dim = pd.concat([x for x, _ in dfs])
    tt_fact = pd.concat([x for _, x in dfs])

    append_to_dim(tt_dim)
    append_to_fact(tt_fact)
