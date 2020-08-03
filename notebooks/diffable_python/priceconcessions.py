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
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import seaborn as sns
from matplotlib.dates import  DateFormatter
# %matplotlib inline
from ebmdatalab import bq
from ebmdatalab import charts
from ebmdatalab import maps

# Here's some text about what we need to do.

# ### Import data from BigQuery

# +
sql = """
  -- First we create a temp table with aggregated data for each bnf_code and month,
  -- which signficantly reduces runtime for the main query

CREATE TEMP TABLE price_concessions_quantity AS
SELECT
  month AS month,
  rx.bnf_code AS bnf_code,
  RTRIM(bnf_name) AS bnf_name,
  SUM(quantity) AS quantity,
  SUM(actual_cost) AS actual_cost,
  AVG(SUM(quantity)) OVER (PARTITION BY rx.bnf_code ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_ave_quantity -- this creates a 3 month rolling average of quantity
FROM
  hscic.normalised_prescribing AS rx
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
  DATE(rx.month) AS rx_month,
  vmpp.bnf_code AS bnf_code,
  vmpp.nm AS product_name,
  SUM(rx.quantity) AS quantity,
  SUM(rx.rolling_ave_quantity) AS rolling_ave_quantity,
  dt.price_pence AS dt_price_pence,
  ncso.price_pence AS ncso_price_pence,
  
  SUM((dt.price_pence * 
       rx_item.quantity *
          -- This uses quantity from 2 months prior to the prescribing month.
          -- This is the data available at the time of the price concession announcement,
          -- so we use this to predict.

      --vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      --ELSE
      1 / vmpp.qtyval
      --END
      -- This is the "discount factor" which applies the National Average 7.2%
      -- discount to estimate Actual Cost from Net Ingredient Cost and also
      -- converts figures from pence to pounds
      * 0.00928)+ (COALESCE(ncso.price_pence - dt.price_pence,
        0) * rx_item.quantity *
      --CASE
      --  WHEN vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      --ELSE
      1 / vmpp.qtyval
      --END
      * 0.00928)) AS predicted_cost,
  SUM((dt.price_pence * rx_item.rolling_ave_quantity * -- this is using 3 months rolling quantity average
      --CASE
      --  WHEN vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      --ELSE
      1 / vmpp.qtyval
    --END
      * 0.00928)+ (COALESCE(ncso.price_pence - dt.price_pence,
        0) * rx_item.rolling_ave_quantity *
      --CASE
        --WHEN vmpp.bnf_code IN ('0206010F0AACJCJ', '1202010U0AAAAAA') THEN 1
      --ELSE
      1 / vmpp.qtyval
    --END
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

exportfile = os.path.join("..","data","ncso_df.csv")
ncso_df = bq.cached_read(sql, csv_path=exportfile, use_cache=False)
ncso_df["predicted_cost"] = pd.to_numeric(ncso_df["predicted_cost"])
ncso_df['rx_month'] = ncso_df['rx_month'].astype('datetime64[ns]')
# -

ncso_sum_df=ncso_df.groupby(['rx_month',])[['quantity','rolling_ave_quantity','predicted_cost','predicted_cost_rolling','actual_cost']].sum()  #group data to show total per month
ncso_sum_df['difference'] = ncso_sum_df['predicted_cost'] - ncso_sum_df['actual_cost']  #calculate difference between predicted and actual
ncso_sum_df['perc_difference'] = ncso_sum_df['difference'] / ncso_sum_df['actual_cost'] #calculate percentage difference
ncso_sum_df.sort_values(by=['rx_month']) #sort values by month for chart

ncso_sum_df.reset_index(inplace=True)

ax = ncso_sum_df.plot.bar(figsize = (12,6), y= ['perc_difference'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

ncso_sum_df['perc_difference'].std()

ncso_sum_df['difference_rolling'] = ncso_sum_df['predicted_cost_rolling'] - ncso_sum_df['actual_cost']  #calculate difference between 3 month average rolling predicted and actual
ncso_sum_df['perc_difference_rolling'] = ncso_sum_df['difference_rolling'] / ncso_sum_df['actual_cost'] #calculate percentage difference on 3 month rolling

ax = ncso_sum_df.plot.bar(figsize = (12,6), y=['perc_difference', 'perc_difference_rolling'])
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs, \nrolling 3 month average forecast and actual spend')
ax.legend(["Single month forecast", "Rolling 3 month forecast"])

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

ncso_sum_df = pd.merge(ncso_sum_df, dates, on='rx_month')

dates.index = pd.to_datetime(dates.index)

#dates['pred_month'] = dates.lookup(dates.index, dates['bdays'])
ncso_sum_df['bdays2']=ncso_sum_df['bdays'].shift(2)

ncso_sum_df.head()

ncso_sum_df['predicted_cost_work_days_adj']=(ncso_sum_df['bdays2']/ncso_sum_df['bdays'])*result['predicted_cost']
ncso_sum_df['difference_work_day_adj']=ncso_sum_df['predicted_cost_work_days_adj']-ncso_sum_df['actual_cost']
ncso_sum_df['percent_difference_work_days_adj']=ncso_sum_df['difference_work_day_adj']/ncso_sum_df['actual_cost']

ax = ncso_sum_df.plot.bar(figsize = (12,6), x='rx_month', y='percent_difference_work_days_adj')

ax = ncso_sum_df.plot.bar(figsize = (12,6), y=['perc_difference', 'perc_difference_rolling', 'percent_difference_work_days_adj'])
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
#ax.set_title('Percentage difference between forecasted price concession costs, \nrolling 3 month average forecast and actual spend')
#ax.legend(["Single month forecast", "Rolling 3 month forecast"])

ncso_sum_df.head(25)

result.describe()


