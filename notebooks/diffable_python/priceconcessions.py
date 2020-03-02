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
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from ebmdatalab import bq
from ebmdatalab import charts
from ebmdatalab import maps

# Here's some text about what we need to do.

# ### Import data from BigQuery

# +
sql = """
  -- first, we create a temp table with aggregated data for each bnf_code and month,
  -- which signfic antly reduces runtime for the main query
  -- first, we create a temp table with aggregated data for each bnf_code and month,
  -- which signficantly reduces runtime for the main query
  CREATE TEMP TABLE price_concessions_quantity AS
SELECT
  month,
  rx.bnf_code AS bnf_code,
  RTRIM(bnf_name) AS bnf_name,
  SUM(quantity) AS quantity,
  SUM(actual_cost) AS actual_cost,
  AVG(SUM(quantity)) OVER (PARTITION BY rx.bnf_code ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_ave_quantity -- this creates a 3 month rolling average of quantity
FROM
  hscic.normalised_prescribing_standard AS rx
INNER JOIN (
  SELECT
    bnf_code
  FROM
    dmd.ncsoconcession AS ncso
  INNER JOIN
    dmd.vmpp AS vmpp
  ON
    vmpp.id=ncso.vmpp
  GROUP BY
    bnf_code) AS ncso
ON
  ncso.bnf_code = rx.bnf_code
GROUP BY
  month,
  rx.bnf_code,
  bnf_name
ORDER BY
  bnf_code,
  month ;
  --this is the main query
SELECT
  rx.month AS rx_month,
  vmpp.bnf_code AS bnf_code,
  vmpp.nm AS product_name,
  SUM(rx.quantity) AS quantity,
  SUM(rx.rolling_ave_quantity) AS rolling_ave_quantity,
  dt.price_pence AS dt_price_pence,
  ncso.price_pence AS ncso_price_pence,
  SUM((dt.price_pence * rx_item.quantity *
      CASE WHEN
      -- This uses quantity from 2 months prior to the prescribing month.
      -- This is the data available at the time of the price concession announcement,
      -- so we use this to predict.
      -- For some presentations "quantity" means "number of packs" rather
      -- than e.g. tablets. In these cases we don't want to divide by the
      -- quantity value of a pack. This is implemented via a flag in our
      -- databse but this data isn't in BiqQuery so we just have a hardcoded
      -- list of BNF codes here
      vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      ELSE
      1 / vmpp.qtyval
    END
      -- This is the "discount factor" which applies the National Average 7.2%
      -- discount to estimate Actual Cost from Net Ingredient Cost and also
      -- converts figures from pence to pounds
      * 0.00928)+ (COALESCE(ncso.price_pence - dt.price_pence,
        0) * rx_item.quantity *
      CASE
        WHEN vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      ELSE
      1 / vmpp.qtyval
    END
      * 0.00928)) AS predicted_cost,
  SUM((dt.price_pence * rx_item.rolling_ave_quantity * -- this is using 3 months rolling quantity average
      CASE
        WHEN vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      ELSE
      1 / vmpp.qtyval
    END
      * 0.00928)+ (COALESCE(ncso.price_pence - dt.price_pence,
        0) * rx_item.rolling_ave_quantity *
      CASE
        WHEN vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      ELSE
      1 / vmpp.qtyval
    END
      * 0.00928)) AS predicted_cost_rolling,
  SUM(rx.actual_cost) AS actual_cost
FROM
  dmd.tariffprice AS dt
RIGHT JOIN
  dmd.ncsoconcession AS ncso
ON
  ncso.vmpp = dt.vmpp
  AND ncso.date = dt.date
INNER JOIN
  dmd.vmpp AS vmpp
ON
  vmpp.id=ncso.vmpp
INNER JOIN
  price_concessions_quantity AS rx --this is joining to the temp table, creating the current month actual_cost
ON
  rx.bnf_code = vmpp.bnf_code
  AND rx.month = TIMESTAMP(ncso.date)
INNER JOIN
  price_concessions_quantity AS rx_item --this is joining to the temp table, with a 2 month difference to calculate predicted quantities
ON
  rx_item.bnf_code = vmpp.bnf_code
  AND rx_item.month = TIMESTAMP(DATE_ADD(ncso.date, INTERVAL -2 month))
WHERE
  rx.month >='2017-01-01'
GROUP BY
  rx.month,
  vmpp.bnf_code,
  vmpp.nm,
  dt.price_pence,
  ncso.price_pence,
  TIMESTAMP(DATE_ADD(ncso.date, INTERVAL -2 month))
ORDER BY
  vmpp.bnf_code,
  rx.month
"""

#exportfile = os.path.join("..","data","ncso_df.csv")
ncso_df = bq.cached_read(sql, csv_path='data/ncso_df.csv')
ncso_df["predicted_cost"] = pd.to_numeric(ncso_df["predicted_cost"])
# -

ncso_sum_df=ncso_df.groupby('rx_month')[['quantity','rolling_ave_quantity','predicted_cost','predicted_cost_rolling','actual_cost']].sum()  #group data to show total per month

ncso_sum_df['difference'] = ncso_sum_df['predicted_cost'] - ncso_sum_df['actual_cost']  #calculate difference between predicted and actual

ncso_sum_df['perc_difference'] = ncso_sum_df['difference'] / ncso_sum_df['actual_cost'] #calculate percentage difference

ax = ncso_sum_df.plot.bar(figsize = (12,6), y='perc_difference')

ncso_sum_df['difference_rolling'] = ncso_sum_df['predicted_cost_rolling'] - ncso_sum_df['actual_cost']  #calculate difference between 3 month average rolling predicted and actual

ncso_sum_df['perc_difference_rolling'] = ncso_sum_df['difference_rolling'] / ncso_sum_df['actual_cost'] #calculate percentage difference on 3 month rolling

ax = ncso_sum_df.plot.bar(figsize = (12,6), y=['perc_difference_rolling', 'perc_difference'])

# ### How could we get more accuracy?

# +
# import bank holidays json (2012-2020)
#from pandas.io.json import json_normalize #package for flattening json in pandas df
# -

# load bank holidays json and pass to busdays function `holidays=[]` ###
url = 'https://www.gov.uk/bank-holidays.json'
bh = pd.read_json(url, orient='index')
# separate out the embedded json 
#flattening json in pandas df
bh2 = pd.json_normalize(bh.iloc[0]["events"])
bh2.head()

ncso_dates_df=ncso_sum_df.reset_index()

import calendar
dates = ncso_dates_df[["rx_month"]].drop_duplicates()
dates["rx_month"] = pd.to_datetime(dates["rx_month"])
dates["year"] = dates["rx_month"].dt.year
dates["mon"] = dates["rx_month"].dt.month
d = []
for row in dates.itertuples():
    y = row.year
    m = row.mon
    day = calendar.monthrange(y,m)[1]
    d.append(str(y)+"-"+str(m)+"-"+str(day))
d = pd.Series(d, name="enddates")
d = pd.to_datetime(d, format="%Y/%m/%d")
begindates = pd.Series(dates["rx_month"]).values.astype('datetime64[D]')
enddates = pd.Series(d).values.astype('datetime64[D]')
#######
# find business days in month
dates["bdays0"] = np.busday_count(begindates, enddates) # not excluding bank holidays
dates["bdays"] = np.busday_count(begindates, enddates, holidays=bh2["date"].values.astype('datetime64[D]'))
dates.head()

dates.set_index('rx_month')

ncso_sum_df.reset_index()

ncso_sum_df.dtypes

result = pd.merge(ncso_sum_df, dates, on='rx_month')

result.dtypes

import calendar
dates = ncso_dates_df[["rx_month"]].drop_duplicates()
dates["rx_month"] = pd.to_datetime(dates["rx_month"])
dates["year"] = dates["rx_month"].dt.year
dates["mon"] = dates["rx_month"].dt.month
d = []
for row in dates.itertuples():
    y = row.year
    m = row.mon
    day = calendar.monthrange(y,m)[1]
    d.append(str(y)+"-"+str(m)+"-"+str(day))
d = pd.Series(d, name="enddates")
d = pd.to_datetime(d, format="%Y/%m/%d")
begindates = pd.Series(dates["rx_month"]).values.astype('datetime64[D]')
enddates = pd.Series(d).values.astype('datetime64[D]')
#######
# find business days in month
dates["bdays0"] = np.busday_count(begindates, enddates) # not excluding bank holidays
dates["bdays"] = np.busday_count(begindates, enddates, holidays=bh2["date"].values.astype('datetime64[D]'))
dates.head()

dates.set_index('rx_month')

dates.index = pd.to_datetime(dates.index)

#dates['pred_month'] = dates.lookup(dates.index, dates['bdays'])
result['bdays2']=result['bdays'].shift(2)

result.head()

result['predicted_cost_adj']=(result['bdays2']/result['bdays'])*result['predicted_cost']

result['difference_adj']=result['predicted_cost_adj']-result['actual_cost']

result['percent_difference_adj']=result['difference_adj']/result['actual_cost']

ax = result.plot.bar(figsize = (12,6), x='rx_month', y='percent_difference_adj')

result.head(25)

result.describe()

result['perc_difference'].mean()


