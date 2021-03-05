#!/usr/bin/env python
# coding: utf-8

# In[1]:


from __future__ import print_function
import datetime
import time
from functools import reduce
from collections import defaultdict
import os
import tqdm
import gzip
import pickle

import pyarrow.parquet as parquet
import pandas as pd
import numpy as np
get_ipython().run_line_magic('matplotlib', 'nbagg')
import matplotlib.pyplot as plt

from fastset import FastSet


# In[2]:


#   DBS BLOCKS table schema:
#     BLOCK_ID NOT NULL NUMBER(38)
#     BLOCK_NAME NOT NULL VARCHAR2(500)
#     DATASET_ID NOT NULL NUMBER(38)
#     OPEN_FOR_WRITING NOT NULL NUMBER(38)
#     ORIGIN_SITE_NAME NOT NULL VARCHAR2(100)
#     BLOCK_SIZE NUMBER(38)
#     FILE_COUNT NUMBER(38)
#     CREATION_DATE NUMBER(38)
#     CREATE_BY VARCHAR2(500)
#     LAST_MODIFICATION_DATE NUMBER(38)
#     LAST_MODIFIED_BY VARCHAR2(500)
if not os.path.exists('data/block_size.npy'):
    # numpy readcsv couldn't handle the size :(
    blocksize = pd.read_csv("data/dbs_blocks.csv.gz", dtype='i8', usecols=(0,5), names=['block_id', 'block_size'])
    np.save('data/block_size.npy', blocksize.values)
    blocksize = blocksize.values
else:
    blocksize = np.load('data/block_size.npy')

# We'll be accessing randomly, make a fast lookup
bsort = np.argsort(blocksize[:,0])
bsize_index = FastSet(blocksize[bsort, 0])
bsize_values = blocksize[bsort, 1]
bsize_values = np.append(bsize_values, 0)
def getsize(s):
    return bsize_values[np.searchsorted(bsize_index._set, s._set)].sum()


# In[3]:


blockmap_filename = 'data/blockmap.pkl.gz'
if not os.path.exists(blockmap_filename):
    blockmap_in = pd.read_csv("data/dbs_blocks.csv.gz", dtype='i8', usecols=(0,2), names=['block_id', 'dataset_id'])
    blockmap = defaultdict(FastSet)
    for bid, dsid in blockmap_in.values:
        blockmap[dsid] += FastSet([bid])

    del blockmap_in
    with gzip.open(blockmap_filename, 'wb') as fout:
        pickle.dump(blockmap, fout)
else:
    with gzip.open(blockmap_filename) as fin:
        blockmap = pickle.load(fin)


# In[4]:


ws_cmssw = parquet.read_table('data/working_set_cmssw').to_pandas()
ws_cmssw['working_set_blocks'] = ws_cmssw.apply(lambda x: FastSet(x.working_set_blocks), 'columns')


# In[25]:


ws_jobm = parquet.read_table('data/working_set_jmpopularity').to_pandas()
ws_jobm['working_set_blocks'] = ws_jobm.apply(lambda x: FastSet(x.working_set_blocks), 'columns')


# In[5]:


ws_classads = parquet.read_table('data/working_set_classads').to_pandas()
ws_classads['working_set_blocks'] = ws_classads.apply(
    lambda x: sum((blockmap[ds] for ds in x.working_set), FastSet()), 'columns')


# In[6]:


ws_xrootd = parquet.read_table('data/working_set_xrootd').to_pandas()
ws_xrootd['working_set_blocks'] = ws_xrootd.apply(lambda x: FastSet(x.working_set_blocks), 'columns')


# In[7]:


ws_fwjr = parquet.read_table('data/working_set_fwjr').to_pandas()
ws_fwjr['working_set_blocks'] = ws_fwjr.apply(lambda x: FastSet(x.working_set_blocks), 'columns')


# In[8]:


#missing_xrootd = ws_xrootd['working_set_blocks'].values.sum() - bsize_index
#print("Missing xrootd blocks:", len(missing_xrootd))
#missing_xrootd._set


# In[26]:


# join the data tier definitions
datatiers = pd.read_csv('data/dbs_datatiers.csv').set_index('id')
def add_datatiers(ws):
    ws['data_tier'] = datatiers.loc[ws.d_data_tier_id].data_tier.values
    
add_datatiers(ws_classads)
add_datatiers(ws_cmssw)
add_datatiers(ws_xrootd)
add_datatiers(ws_fwjr)
add_datatiers(ws_jobm)


# In[10]:


date_index = np.arange(1451606400//86400, time.time()//86400 - 1)
date_index_ts = np.array(list(datetime.date.fromtimestamp(day*86400) for day in date_index))


# In[11]:


def calc(ws_filtered):
    blocks_day = []
    for i, day in enumerate(tqdm.tqdm(date_index, desc='Assemble block lists', unit='day')):
        today = (ws_filtered.day==day*86400)
        blocks_day.append(reduce(FastSet.union, ws_filtered[today].working_set_blocks, FastSet()))

    nrecords = np.zeros_like(date_index)
    lifetimes = {
        '1w': 7,
        '1m': 30,
        '3m': 90,
        '6m': 120,
    }
    ws_size = {k: np.zeros_like(date_index) for k in lifetimes}
    nrecalls = {k: np.zeros_like(date_index) for k in lifetimes}
    recall_size = {k: np.zeros_like(date_index) for k in lifetimes}
    previous = {k: FastSet() for k in lifetimes}

    for i, day in enumerate(tqdm.tqdm(date_index, desc='Simulating days', unit='day')):
        nrecords[i] = ws_filtered[(ws_filtered.day==day*86400)].size
        for key in lifetimes:
            current = reduce(FastSet.union, blocks_day[max(0,i-lifetimes[key]):i+1], FastSet())
            recall = current - previous[key]
            nrecalls[key][i] = len(recall)
            ws_size[key][i] = getsize(current)
            recall_size[key][i] = getsize(recall)
            previous[key] = current

    return ws_size, nrecalls, recall_size


# In[27]:


def filt(ws):
    # return ws
    return ws[ws['data_tier'].str.match('(|MINI|NANO)AOD')]

# size_cmssw, _, _ = calc(filt(ws_cmssw))
# size_classads, _, _ = calc(filt(ws_classads))
# size_xrootd, _, _ = calc(filt(ws_xrootd))
# size_fwjr, _, _ = calc(filt(ws_fwjr))
size_jobm, _, _ = calc(filt(ws_jobm))
size_all, _, _ = calc(filt(pd.concat([ws_cmssw, ws_classads, ws_xrootd, ws_fwjr, ws_jobm], sort=False)))


# In[31]:


def mask_valid(line, ws):
    mask = (ws.day.unique()//86400 - date_index[0]).astype(int)
    mask = mask[(mask>0)&(mask<date_index.size)]  # super old xrootd??
    out = np.full_like(line, np.nan)
    out[mask] = line[mask]
    return out

window = '1w'
fig, ax = plt.subplots(1,1, figsize=(10,5))
ax.plot(date_index_ts, mask_valid(size_cmssw[window], ws_cmssw)/1e15, label='CMSSW')
ax.plot(date_index_ts, mask_valid(size_classads[window], ws_classads)/1e15, label='ClassAds')
ax.plot(date_index_ts, mask_valid(size_xrootd[window], ws_xrootd)/1e15, label='XRootD')
ax.plot(date_index_ts, mask_valid(size_fwjr[window], ws_fwjr)/1e15, label='WMStats fwjr')
ax.plot(date_index_ts, mask_valid(size_jobm[window], ws_jobm)/1e15, label='JobMonitoring')
ax.plot(date_index_ts, size_all[window]/1e15, label='Union', color='k', linewidth=2)


ax.legend(title='Data source')
ax.set_title('Working set comparison, *AOD data tiers')
ax.set_ylabel('Working set size [PB]')
ax.set_xlabel('Date (%s window)' % window)
ax.set_ylim(0, None)
#ax.set_xlim(datetime.date(2016, 1, 1), None)

fig.savefig("compare_workingset_aod_%s.pdf" % window)


# In[ ]:




