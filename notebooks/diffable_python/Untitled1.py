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
#import seaborn as sns
#from matplotlib.dates import  DateFormatter
# %matplotlib inline
from ebmdatalab import bq
from ebmdatalab import charts
#from ebmdatalab import maps
#import datetime as dt

# **TEST DATA**

# import data from original query https://stackoverflow.com/questions/26911851/how-to-use-pandas-to-find-consecutive-same-data-in-time-series
importfile = os.path.join("..","data","test_cons_5.csv") #defines name for cache file
syn_df = pd.read_csv(importfile)
syn_df['month'] = pd.to_datetime(syn_df['month'])
syn_df.style





syn_big_df = syn_df.set_index(['month', 'vmpp']).unstack().asfreq('MS').fillna(0).stack().sort_index(level=1).reset_index()





# +
con_months_df = (syn_big_df.assign(Consecutive=syn_big_df.concession_bool
                                .groupby((syn_big_df.concession_bool != syn_big_df.concession_bool.shift())
                                         .cumsum())
                                .transform('size'))
          .query('Consecutive > 1')
          .groupby('Consecutive')
          .aggregate(vmpp=('vmpp','first'),
                     first_month=('month','first'), 
                     last_month=('month','last'),
                     bool_total=('concession_bool','sum'))
          .reset_index()
)

#con_months_df.columns = [t[1] if t[1] else t[0] for t in con_months_df.columns]
con_months_df

# -

# ** PRICE CONCESSION DATA **

# +
sql = """
  SELECT DISTINCT
    ncso.vmpp AS vmpp,
    ncso.date AS month,    
    1 AS concession_bool
  FROM
    ebmdatalab.dmd.ncsoconcession AS ncso --concession table
"""

exportfile = os.path.join("..","data","ncso_dates.csv") #defines name for cache file
dates_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
dates_df['month'] = pd.to_datetime(dates_df['month'])#ensure dates are in datetimeformat
# -

dates_df = dates_df.sort_values(by=['month','vmpp'])

# +
#dates_cons_df = dates_df.set_index(['month', 'vmpp']).unstack().asfreq('MS', fill_value=0).stack().sort_index(level=1).reset_index()
# -

#dates_cons_df = dates_df.set_index(['month', 'vmpp']).unstack().asfreq('MS', fill_value=0)
dates_cons_df = dates_df.set_index(['month', 'vmpp']).unstack().asfreq('MS').fillna(0).stack().sort_index(level=1).reset_index()

dates_cons_df.head()

# +
con_months_df = (dates_cons_df.assign(Consecutive=dates_cons_df.concession_bool
                                .groupby(('dates_cons_df.concession_bool != dates_cons_df.concession_bool.shift())
                                         .cumsum())
                                .transform('size'))
          .query('Consecutive > 1')
          .groupby('Consecutive')
          .aggregate(vmpp=('vmpp','first'),
                     first_month=('month','first'), 
                     last_month=('month','last'),
                     bool_total=('concession_bool','sum'))
          .reset_index()
)

#con_months_df.columns = [t[1] if t[1] else t[0] for t in con_months_df.columns]
con_months_df.style
# +
dates_df.head()
# -


# +
con_months_df = dates_cons_df.query('concession_bool >0')

#con_months_df["new_date"] = con_months_df.month + pd.offsets.MonthOffset(-1)
#con_months_df =  con_months_df[con_months_df['concession_bool']!=0]

#on_months_df.groupby(['vmpp', 'Consecutive']).agg(first_month=('month','first'),last_month=('month','last'))
#aggregate(vmpp=('vmpp','first'),first_month=('month','first'),last_month=('month','last'),bool_total=('concession_bool','sum')).reset_index()


# +
con_months_df = (dates_cons_df.assign(Consecutive=dates_cons_df.concession_bool
                                .groupby((dates_cons_df.concession_bool != dates_cons_df.concession_bool.shift())
                                         .cumsum()
                                .transform('size'))).query('concession_bool >0')
#con_months_df =  con_months_df[con_months_df['concession_bool']!=0]

#on_months_df.groupby(['vmpp', 'Consecutive']).agg(first_month=('month','first'),last_month=('month','last'))
#aggregate(vmpp=('vmpp','first'),first_month=('month','first'),last_month=('month','last'),bool_total=('concession_bool','sum')).reset_index()

# -

con_months_df["in_vle"] = con_months_df.index

con_months_df["grouping"] = con_months_df.groupby(dates_cons_df.concession_bool != dates_cons_df.concession_bool.shift()).cumsum().add(1)

hi = con_months_df.groupby('vmpp')

con_months_df.head(200)



# +
dates_cons_df.style
# -


rslt = (dates_cons_df.assign(Consecutive=dates_cons_df.concession_bool
                                .groupby((dates_cons_df.concession_bool != dates_cons_df.concession_bool.shift())
                                         .cumsum())
                                .transform('size'))
          .query('concession_bool > 0')
          .groupby(['vmpp','Consecutive'])
          .aggregate(first_month=('month','first'), 
                     last_month=('month','last'))
          .reset_index().query("last_month < '2022-09-01'")
)
rslt





















rslt.style











select * from `ebmdatalab.dmd.tariffprice`

# +
sql = """
  SELECT *

  FROM
    ebmdatalab.dmd.tariffprice
"""

exportfile = os.path.join("..","data","tariff.csv") #defines name for cache file
dates_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
dates_df['date'] = pd.to_datetime(dates_df['date'])#ensure dates are in datetimeformat
# -

dates_df.head()

dates_df['pre_month'] = dates_df['date'] + pd.DateOffset(months=1)
dates_df['post_month'] = dates_df['date'] + pd.DateOffset(months=-3)
dates_df['3_month_price'] = dates_df.groupby('vmpp')['price_pence'].transform(lambda x: x.rolling(3, 3).mean())

# +
dates_df_merge = pd.merge(rslt, dates_df[['vmpp','pre_month','3_month_price']],  how='left', left_on=['vmpp','first_month'], right_on = ['vmpp','pre_month'])
dates_df_merge.rename(columns={'3_month_price' : 'pre_pc_price'}, inplace=True)
#dates_df_merge = pd.merge(dates_df_merge, dates_df[['vmpp','post_month','3_month_price']],  how='left', left_on=['vmpp','last_month'], right_on = ['vmpp','post_month'])
# -


dates_df_merge = pd.merge(dates_df_merge, dates_df[['vmpp','post_month','3_month_price']],  how='left', left_on=['vmpp','last_month'], right_on = ['vmpp','post_month'])

dates_df_merge.query('vmpp == 1290011000001107')







# +
data1 = {'date': ['2019-06-10', '2019-06-11', '2019-06-17', '2019-06-18'], 'age': [20, 21, 19, 18]}

data1['date']=pd.to_datetime(data1['date'])

df1 = pd.DataFrame(data1)

df1.set_index('date', inplace=True)

data2 = {'wk start': ['2019-06-10', '2019-06-17', '2019-06-24', '2019-07-02'], 'wk end':[ '2019-06-14', '2019-06-21', '2019-06-28', '2019-07-05'], 'height': [120,121, 119, 118]}

data2['wk start']=pd.to_datetime(data2['wk start'])

data2['wk end']=pd.to_datetime(data2['wk end'])

df2 = pd.DataFrame(data2)

# Loop
list1 = []
for row in df1.iterrows():
    subdf = df2[(df2['wk start'] <= index) & (df2['wk end'] >= index)]
    list1.append(subdf['height'].tolist()[0])
df1['height'] = list1
print(df1)
# -


