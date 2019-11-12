#!/usr/bin/env python

''' Analyze xrootd parquet files '''

from pathlib import Path
import pyarrow.parquet as parquet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re


def loop_parquets(data_dir):

    ''' Main loop. Reads parquet files, creates grouped dataframe.
    data_dir: Directory with parquet files
    Return value: sorted, grouped dataframe
    '''

    #df = pd.DataFrame()
    parquets = list(data_dir.glob('*.parquet'))
    for idx, parquet_file in enumerate(parquets):
        print('Reading file {} from {}.'.format(idx+1, len(parquets)))
        xrootd_df = pd.read_parquet(parquet_file)
        #data_dir = Path('parquets')
        #xrootd_df = pd.concat(
        #    [pd.read_parquet(parquet_file) for parquet_file in
        #        data_dir.glob('*.parquet')],
        #    sort=False, ignore_index=True
        #)

        # Truncate, for debugging
        #print('We are truncating the parquet files!')
        #xrootd_df = xrootd_df.sort_index()
        #xrootd_df = xrootd_df.truncate(before=1, after=2)
        print(xrootd_df)

def plot_overview(df):

    ''' Overview plot, showing number of transfers vs. time.
    df: input dataframe '''

    # Group for overview plot
    dfg = (df
            .groupby(['xrootd_date_ym', 'location_source', 'location_target'])
            .sum()
            .reset_index()
            )
    #print('Foo01')
    #print(type(df))
    #print(df.head(10))
    #print('Foo02')
    #print(type(dfg))
    #print(dfg.head(10))

    # Get legend for plot
    leg = []
    for loc, _ in dfg.groupby(['location_source', 'location_target']):
        leg.append('from {} to {}'.format(loc[0], loc[1]))

    # Plot content of pandas, row by row
    fig, ax = plt.subplots(1,1, figsize=(10,5))
    dfg.groupby(['location_source', 'location_target']).plot(x='xrootd_date_ym', y='count',
            ax=ax, kind='line', title='xrootd usage', legend=True, logy=False)
    ax.legend(leg)
    ax.set_xlabel('year-month')
    ax.set_ylabel('number of transfers')

    plt.savefig('data_transfers.pdf')

def plot_transatlantic(df, src, trg):

    ''' Plot transatlantic (east to west, or west to east) transfers, showing
    number of transfers vs. time, split into categories if block would have
    been available on same side of atlantic or not.
    df: input dataframe
    src: source (east or west)
    trg: target (east or west) '''

    # Get source to target transfers
    df_we = df[(df.location_source == src) & (df.location_target == trg)]
    #print('Foo11: df')
    #print(type(df))
    #print(df.head(10))
    #print('Foo13: df_we')
    #print(type(df_we))
    #print(df_we.head(10))

    fig, ax = plt.subplots(1,1, figsize=(10,5))
    df_we.groupby('dataset_at_source').plot(x='xrootd_date_ym', y='count',
            ax=ax, kind='line', title='xrootd {} to {} transfers'.format(src,
                trg), legend=True, logy=False)
    leg = []
    for loc, _ in df_we.groupby('dataset_at_source'):
        if loc:
            leg.append('block available in {} = unnecessary {}-{} transfer'.format(src, src, trg))
        else:
            leg.append('block not available in {} = necessary {}-{} transfer'.format(src, src, trg))

    ax.legend(leg)
    ax.set_xlabel('year-month')
    ax.set_ylabel('number of transfers')
    plt.savefig('data_transfers_{}{}.pdf'.format(src[0], trg[0]))



if __name__ == '__main__':

    # Dictionary with domain locations
    d_locations = {
        'east': ['at', 'be', 'ch', 'de', 'es', 'fi', 'fr', 'gr', 'hu',
                 'in', 'it', 'kr', 'pk', 'pl', 'ru', 'su', 'tr', 'tw',
                 'ua', 'uk'],
        'west': ['us', 'br', 'edu', 'gov', 'org'],
        'unknown': ['ufhpc', 'unknown']
        }

    # Get parquet files and convert to pandas
    # Suffixes like e.g. _2019-01 are appended automatically
    data_dir = Path('test54')
    df = loop_parquets(data_dir)

    # Plot data in df
    plot_overview(df)
    plot_transatlantic(df, 'west', 'east')
    plot_transatlantic(df, 'east', 'west')

    # Get east to west transfers
    #dfg = (df
    #        .groupby(['xrootd_date_ym', 'location_source', 'location_target', 'dataset_at_source'])
    #        .size()
    #        .unstack(fill_value=0)
    #        .stack()
    #        .reset_index()
    #        )
    #xrootd_grp_ew = xrootd_grp_xx[(xrootd_grp_xx.location_source == 'east') & (xrootd_grp_xx.location_target == 'west')]
    #print(xrootd_grp_ew)
    #fig, ax = plt.subplots(1,1, figsize=(10,5))
    #xrootd_grp_ew.groupby('dataset_at_source').plot(x='xrootd_date_ym', y='count',
    #        ax=ax, kind='line', title='xrootd east to west transfers', legend=True, logy=False)
    #leg = ['block not available in east = necessary east-west transfer',
    #       'block available in east = unnecessary east-west transfer']
    #ax.legend(leg)
    #ax.set_xlabel('year-month')
    #ax.set_ylabel('number of transfers')
    #plt.savefig('data_transfers_ew.pdf')



    ## filter by complete=y
