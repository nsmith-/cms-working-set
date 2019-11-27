#!/usr/bin/env python

''' Analyze xrootd parquet files '''

from pathlib import Path
from glob import glob
import pyarrow.parquet as parquet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re


def loop_parquets(data_dir):

    ''' Reads parquet files, creates grouped dataframe.
    data_dir: Directory with parquet files
    Return value: sorted, grouped dataframe
    '''

    # DataFrame to return
    df = pd.DataFrame()

    for year in [2019]:
        for month in range(2, 4):

            # Zero padding month
            month = str(month).zfill(2)

            # Read and concatenate parquet files
            xrootd_df = pd.concat(
                [
                    pd.read_parquet(parquet_file) for parquet_file in
                    glob('{}_{}-{}/*.parquet'.format(data_dir, year, month))
                ],
                sort=False, ignore_index=True
            )

            # Add date info
            xrootd_df['date'] = '{}-{}'.format(year, month)

            # Append new DataFrame
            df = df.append(xrootd_df, ignore_index=True)

    #print(df)
    return df



def plot_overview(df):

    ''' Overview plot, showing number of transfers vs. time.
    df: input dataframe '''

    # Group for overview plot
    dfg = (df
            .drop('dataset_at_client_location', 1)
            .groupby(['date', 'server_location', 'client_location'])
            .sum()
            .reset_index()
            )
    #print('Foo01')
    #print(type(df))
    #print(df)
    #print('Foo02')
    #print(type(dfg))
    #print(dfg)

    # Get legend for plot
    leg = []
    for loc, _ in dfg.groupby(['client_location', 'server_location']):
        leg.append('from {} to {}'.format(loc[0], loc[1]))

    # Plot content of pandas, row by row
    fig, ax = plt.subplots(1,1, figsize=(10,5))
    dfg.groupby(['client_location', 'server_location']).plot(x='date', y='total_read_bytes',
            ax=ax, kind='line', title='xrootd usage', legend=True, logy=False)
    ax.legend(leg)
    ax.set_xlabel('year-month')
    ax.set_ylabel('number of transfers')

    plt.savefig('data_transfers.pdf')

def plot_transatlantic(df, client, server):

    ''' Plot transatlantic (eu to us, or us to eu) transfers, showing
    number of transfers vs. time, split into categories if block would have
    been available on same side of atlantic or not.
    df: input dataframe
    client: source (eu or us)
    server: target (eu or us) '''

    # Get source to target transfers
    df_we = df[(df.client_location == client) & (df.server_location == server)]
    print('Foo11: df')
    print(type(df))
    print(df.head(10))
    print('Foo13: df_we')
    print(type(df_we))
    print(df_we.head(10))

    fig, ax = plt.subplots(1,1, figsize=(10,5))
    df_we.groupby('dataset_at_client_location').plot(x='date', y='total_read_bytes',
            ax=ax, kind='line', title='client: {}; server: {}'
            .format(client, server), legend=True, logy=False)
    leg = []
    for loc, _ in df_we.groupby('dataset_at_client_location'):
        if loc:
            leg.append('block available in {} = unnecessary {}-{} transfer'
                       .format(client, client, server))
        else:
            leg.append('block not available in {} = necessary {}-{} transfer'
                       .format(client, client, server))

    ax.legend(leg)
    ax.set_xlabel('year-month')
    ax.set_ylabel('number of transfers')
    plt.savefig('data_transfers_{}{}.pdf'.format(client[0], server[0]))



if __name__ == '__main__':

    # Show full dataframe, for debugging (but it won't hurt, leaving it here)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', -1)

    # Get parquet files and convert to pandas
    # Suffixes like e.g. _2019-01 are appended automatically
    data_dir = Path('test54')
    df = loop_parquets(data_dir)

    # Plot data in df
    plot_overview(df)
    plot_transatlantic(df, 'us', 'eu')
    plot_transatlantic(df, 'eu', 'us')
