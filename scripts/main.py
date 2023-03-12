# system
import time
import os
import logging

# general
from dotenv import dotenv_values
import pandas as pd
import sqlite3

# third party
from utils.rutracker import Rutracker

DB_PATH = "./data/local.db"

logger = logging.getLogger(__name__)


def search_rt(sort_by:str):
    config = {
        **dotenv_values(".env"),
        **os.environ
    }
    rt = Rutracker(
        login=config['RUTRACKER_LOGIN'],
        password=config['RUTRACKER_PASSWORD'],
        tracker_url='https://rutracker.org/',
        # logging_mode='console'
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
    __df = df.drop_duplicates(subset=['id'], keep='last')
    __df.to_sql(
        name='torrent_dim',
        con=sqlite3.connect(DB_PATH),
        if_exists='append'
    )


def append_to_fact(df:pd.DataFrame):
    df.drop_duplicates(subset=['id'], keep='last', inplace=True)
    df['fact_timest'] = int(time.time())
    df.to_sql(
        name='torrent_fact',
        con=sqlite3.connect(DB_PATH),
        if_exists='append'
    )


def get_torrent_ids() -> list[int]:
    try:
        return pd.read_sql(
            sql='select distinct id from torrent_dim;',
            con=sqlite3.connect(DB_PATH),
        )['id'].tolist()
    except:
        return []


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    dfs = [
        search_rt(sort_by=mode)
        for mode in ['number_of_seeds', 'number_of_leeches', 'date_added']
    ]
    df_downloaded = pd.concat(dfs)
    df_downloaded.drop_duplicates(subset=['id'], keep='last', inplace=True)
    df_downloaded.to_csv('./data/cache.csv', index=False)

    tt_dim, tt_fact = split_to_dim_and_fact(df_downloaded)

    existed_ids = get_torrent_ids()
    tt_dim_filtered = tt_dim[~tt_dim['id'].isin(existed_ids)]
    logger.info(f'Nex torrents count: {tt_dim_filtered.shape[0]}')
    logger.info(f'Get fact by {tt_fact.shape[0]} torrents')

    append_to_dim(tt_dim_filtered)
    append_to_fact(tt_fact)
