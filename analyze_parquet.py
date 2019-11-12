#!/usr/bin/env python

''' Analyze xrootd parquet files '''

from pathlib import Path
import pyarrow.parquet as parquet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re

# TODO: Check the locations again!


def add_domain_ending(df, domain):

    ''' Add domain ending to dataframe. I.e. if domain is fnal.gov, domain
    ending is gov
    df: dataframe
    domain: dataframe column of which domain ending is added '''

    # Given string domain, find domain ending
    def get_domain_ending(domain):
        return domain[domain.rfind('.')+1:]

    # Location where to put the new column
    #loc = df.columns.get_loc(domain)

    df['{}_ending'.format(domain)] = df[domain].apply(get_domain_ending)
    #print(df[[domain, '{}_ending'.format(domain)]])

def add_location(df, domain, loc, d_locations):

    ''' Add location (east, west, unknown) to dataframe by looking at domain
    ending
    df: dataframe
    domain: domain_ending (can be a list)
    loc: name of column '''

    # Given domain ending, find location
    def get_location(domain):

        # Given value, find key
        # Maybe worthwhile to invert the logic of the dictionary above, but it
        # feels more natural like this
        # We distinguish here if domain is a list or not
        if type(domain)==list:
            locations = []
            for d in domain:
                for key, val in d_locations.items():
                    if d.lower() in val:
                        locations.append(key)
                        break
                else:
                    # If we reach here, we didn't find the value in the dictionary,
                    # which is bad. We throw a warning and return "unknown".
                    print('Cannot find {} in dictionary. Return unknown.'.format(d))
                    d_locations['unknown'].append(d.lower())
                    locations.append('unknown')
            return list(set(locations))

        else:
            for key, val in d_locations.items():
                if domain.lower() in val:
                    return key
            # If we reach here, we didn't find the value in the dictionary, which
            # is bad. We throw a warning, and return "unknown".
            print('Cannot find {} in dictionary. Return unknown.'.format(domain))
            d_locations['unknown'].append(domain.lower())
            return 'unknown'

    #print(df[domain])
    df[loc] = df[domain].apply(get_location)
    #print(df[[domain, loc]])

def add_sources(df):

    ''' Add all possible sources of block to dataframe.
    df: dataframe '''

    def get_sources(sites):
        locations = []
        for site in sites:
            m = re.search('_([^_]+)_', site)
            locations.append(m.group(1))
        return list(set(locations))

    df['domains_with_dataset'] = df['sites_with_dataset'].apply(get_sources)
    #print(df[['sites_with_dataset', 'domains_with_dataset']])

def add_dataset_at_source(df):

    ''' Add if dataset is at source or not.
    df: dataframe '''

    def dataset_at_source(row):
        #locations_with_dataset
        return (row['location_source'] in row['locations_with_dataset'])

    df['dataset_at_source'] = df.apply(dataset_at_source, axis=1)

def add_ym(df):

    ''' Add year-month to dataframe.
    df: dataframe '''

    # Given string domain, find domain ending
    def get_ym(date):
        return date[:7]

    # Location where to put the new column
    #loc = df.columns.get_loc(domain)

    df['xrootd_date_ym'] = df['xrootd_date'].apply(get_ym)
    #print(df[['xrootd_date', 'xrootd_date_ym']])

#@profile
def loop_parquets(data_dir):

    ''' Main loop. Reads parquet files, creates grouped dataframe.
    data_dir: Directory with parquet files
    Return value: sorted, grouped dataframe
    '''

    df = pd.DataFrame()
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
        #print(xrootd_df)

        # Extract locations in pandas
        #print(xrootd_df)
        add_domain_ending(xrootd_df, 'server_domain')
        add_domain_ending(xrootd_df, 'client_domain')
        #print(xrootd_df)
        add_location(xrootd_df, 'server_domain_ending', 'location_target', d_locations)
        add_location(xrootd_df, 'client_domain_ending', 'location_source', d_locations)
        #print(xrootd_df)
        #print(xrootd_df.columns)
        add_sources(xrootd_df)
        add_location(xrootd_df, 'domains_with_dataset', 'locations_with_dataset', d_locations)
        #print(xrootd_df[['location_source', 'location_target', 'locations_with_dataset']])
        add_dataset_at_source(xrootd_df)
        #print(xrootd_df[['location_source', 'locations_with_dataset', 'dataset_at_source']])


        # Extract dates in pandas
        add_ym(xrootd_df)

        # Count data transfers
        xrootd_grp = (xrootd_df
                .groupby(['xrootd_date_ym', 'location_source', 'location_target', 'dataset_at_source'])
                .size()
                .unstack(fill_value=0)
                .stack()
                .reset_index(name='count')
                )
        df = pd.concat([df, xrootd_grp], ignore_index=True)
        del xrootd_df
        del xrootd_grp
        #print(xrootd_grp.columns)

    # Need to group dataframe again, since data from different parquet files is
    # not grouped
    #print('Foo01')
    #print(type(df))
    #print(df.head(10))
    df = (df
            .groupby(['xrootd_date_ym', 'location_source', 'location_target', 'dataset_at_source'])
            .sum()
            .reset_index()
            )
    #print('Foo02')
    #print(type(df))
    #print(df.head(30))

    # Sorting not needed after grouping above?
    #df = df.sort_values(by=['xrootd_date_ym'])
    #df = df.reset_index(drop=True)
    #print(df.head(10))
    return df

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
    data_dir = Path('parquets')
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
