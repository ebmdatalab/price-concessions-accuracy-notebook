# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import os
import pandas as pd
import numpy as np
#import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
# %matplotlib inline
from ebmdatalab import bq
from ebmdatalab import charts

# **PRICE CONCESSION DATA**

#get price concession data from BigQuery
sql = """
  SELECT DISTINCT
    ncso.vmpp AS vmpp,
    ncso.date AS month,    
    1 AS concession_bool --creates a boolean value to show a price concession exists
  FROM
    ebmdatalab.dmd.ncsoconcession AS ncso --concession table
"""
exportfile = os.path.join("..","data","ncso_dates.csv") #defines name for cache file
dates_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
dates_df['month'] = pd.to_datetime(dates_df['month']) #ensure dates are in datetimeformat
dates_df = dates_df.sort_values(by=['month','vmpp']) #sort data by month then vmpp

#unstacks data, fills missing month data (with zero value where no concession), then restacks
dates_cons_df = dates_df.set_index(['month', 'vmpp']).unstack().asfreq('MS').fillna(0).stack().sort_index(level=1).reset_index()

max_date = dates_cons_df["month"].max() + pd.DateOffset(months=-3) #creates variable to ensure that all price concession data have three months after concession ends to ensure calculation of change
pc_summary_df = (dates_cons_df.assign(Consecutive=dates_cons_df.concession_bool
                                .groupby((dates_cons_df.concession_bool != dates_cons_df.concession_bool.shift())
                                    .cumsum()).transform('size')) #creates a value of the number of consecutive months of either price concession or no price concession
          .query('concession_bool > 0') # filters to only where price concession is present 
          .groupby(['vmpp','Consecutive'])
          .aggregate(first_month=('month','first'),  #shows earliest month of consecutive price concession
                     last_month=('month','last')) #shows latest month of consecutive price concession
          .reset_index().query("last_month < @max_date")
)
######THIS IS NOT QUITE WHAT I NEED - IDEALLY HAVE A UNIQUE NUMBER EVERY TIME ONE OF THE GROUPBY CHANGES, IN ORDER TO ENSURE EVERY ONE IS PICKED UP######

# +
#get drug tariff price data from BigQuery
sql = """
  SELECT *
  FROM
    ebmdatalab.dmd.tariffprice
"""

exportfile = os.path.join("..","data","tariff.csv") #defines name for cache file
dates_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
dates_df['date'] = pd.to_datetime(dates_df['date'])#ensure dates are in datetimeformat
# -

dates_df['pre_month'] = dates_df['date'] + pd.DateOffset(months=1) #creates extra date column in drug tariff price shifted by one month later, to pick up 3 month rolling mean spend for the month before price concession added
dates_df['post_month'] = dates_df['date'] + pd.DateOffset(months=-3) #creates extra date column in drug tariff price shifted by three months earlier, to pick up 3 month rolling mean spend for the 3 months after price concession added
dates_df['3_month_price'] = dates_df.groupby('vmpp')['price_pence'].transform(lambda x: x.rolling(3, 3).mean()) # create three month rolling average drug tariff cost

# +
dates_df_merge = pd.merge(pc_summary_df, dates_df[['vmpp','pre_month','3_month_price']],  how='left', left_on=['vmpp','first_month'], right_on = ['vmpp','pre_month']) #merges price concession information with the 3 month average DT price prior to the start of the price concession
dates_df_merge.rename(columns={'3_month_price' : 'pre_pc_price'}, inplace=True) #rename columns
dates_df_merge = pd.merge(dates_df_merge, dates_df[['vmpp','post_month','3_month_price']],  how='left', left_on=['vmpp','last_month'], right_on = ['vmpp','post_month']) #merges price concession information with the 3 month average DT price after the end of the price concession
dates_df_merge.rename(columns={'3_month_price' : 'post_pc_price'}, inplace=True) #rename columns
dates_df_merge = dates_df_merge.drop(columns=['pre_month', 'post_month']) #drop unneccesary columns
dates_df_merge = dates_df_merge.sort_values(by=['vmpp','first_month']) #sort data by month then vmpp
dates_df_merge['perc_difference'] = (dates_df_merge['post_pc_price']/dates_df_merge['pre_pc_price']-1)
dates_df_merge = dates_df_merge.sort_values(by=['perc_difference'], ascending=False) #sort data by month then vmpp
# -


dates_df_merge.style
