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
import datetime



# Here's some text about what we need to do.

# ### Import data from BigQuery

# +
sql = """
WITH 
  price_concession AS (--subquery to remove duplicates due to different pack sizes
  SELECT
    ncso.date AS month, --month
    ncso.drug AS name,  -- drug name
    vmpp.bnf_code AS bnf_code, --BNF code from VMPP table
    ncso.price_pence AS pc_price_pence, --price concession cost per pack
    dt.price_pence AS dt_price_pence, --Drug Tariff cost per pack
    qtyval, --VMPP pack size
    (ncso.price_pence - dt.price_pence)/qtyval AS increased_ppu --difference between concession and usual Drug Tariff price
  FROM
    ebmdatalab.dmd.ncsoconcession AS ncso --concession table
  INNER JOIN
    dmd.vmpp_full AS vmpp --VMPP table
  ON
    ncso.vmpp = vmpp.id
  INNER JOIN
    dmd.tariffprice AS dt -- Drug Tariff table
  ON
    ncso.vmpp = dt.vmpp
    AND ncso.date = dt.date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ncso.date, vmpp.bnf_code ORDER BY (ncso.price_pence - dt.price_pence)/qtyval DESC) = 1 -- for each bnf_code and pack size, calculates PPU difference and ranks in order. Takes the top value, therefore only keeping the highest impact pack size, and thereby removes duplicates for pack size
  ORDER BY
    ncso.date,
    vmpp.bnf_code),
  rx_data AS (--subquery to create prescribing calculations)
  SELECT
    rx.month AS month,
    bnf_name,
    bnf_code AS bnf_code,
    SUM(quantity) AS quantity,
    SUM(net_cost) AS nic,
    SUM(actual_cost) AS actual_cost
  FROM
    ebmdatalab.hscic.normalised_prescribing AS rx
  GROUP BY
    rx.month,
    bnf_name,
    bnf_code)

#main query

SELECT
  rx.month,
  rx.bnf_name,
  rx.bnf_code,
  rx.quantity AS quantity,
  #rx_old.quantity AS quantity_2_months_previously ,
  rx.nic,
  rx.actual_cost,
  dt_price_pence/(100*qtyval) AS normal_actual_cost_per_unit, --calculates "normal" drug tariff price per unit
  pc_price_pence/(100*qtyval) AS predicted_actual_cost_per_unit -- calculates price concession predicted cost per unit
FROM
  rx_data AS rx
#INNER JOIN
#  rx_data AS rx_old -- data from two months previously
#ON
#  rx.bnf_code = rx_old.bnf_code
#  AND DATE(rx.month) = DATE_ADD(DATE(rx_old.month), INTERVAL 2 month) -- join to create data from two months ago
INNER JOIN
  price_concession AS ncso
ON
  DATE(rx.month) = ncso.month
  AND rx.bnf_code = ncso.bnf_code
ORDER BY
  rx.month
"""

exportfile = os.path.join("..","data","ncso_df.csv")
ncso_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True)
ncso_df['month'] = ncso_df['month'].astype('datetime64[ns]')
# -

ncso_df.head()
#max(ncso_df['month'])

op_df = pd.read_csv(os.path.join("..","data","price-concessions-cost-nhs-england-2022-07-01.csv"))
ncso_jul_22_df = ncso_df[ncso_df['month'].dt.strftime('%Y-%m-%d') == "2022-07-01"]
check_ncso_jul_df = pd.merge(ncso_jul_22_df, op_df, left_on='bnf_code', right_on='BNF code')
check_ncso_jul_df = check_ncso_jul_df[['bnf_name','bnf_code','quantity', 'Quantity']]
check_ncso_jul_df = check_ncso_jul_df.rename(columns={"quantity": "bq_quantity", "Quantity": "op_quantity"})
check_ncso_jul_df["difference"] = check_ncso_jul_df["bq_quantity"] - check_ncso_jul_df["op_quantity"]
check_ncso_jul_df = check_ncso_jul_df.sort_values(by=['difference'])
exportfile = os.path.join("..","data","july_difference.csv")
check_ncso_jul_df.to_csv(exportfile, index=False)

op_df = pd.read_csv(os.path.join("..","data","price-concessions-cost-nhs-england-2022-06-01.csv"))
ncso_jun_22_df = ncso_df[ncso_df['month'].dt.strftime('%Y-%m-%d') == "2022-06-01"]
check_ncso_jun_df = pd.merge(ncso_jun_22_df, op_df, left_on='bnf_code', right_on='BNF code')
check_ncso_jun_df = check_ncso_jun_df[['bnf_name','bnf_code','quantity', 'Quantity']]
check_ncso_jun_df = check_ncso_jun_df.rename(columns={"quantity": "bq_quantity", "Quantity": "op_quantity"})
check_ncso_jun_df["difference"] = check_ncso_jun_df["bq_quantity"] - check_ncso_jun_df["op_quantity"]
check_ncso_jun_df = check_ncso_jun_df.sort_values(by=['difference'])
exportfile = os.path.join("..","data","june_difference.csv")
check_ncso_jun_df.to_csv(exportfile, index=False)



#eck_ncso_list_df.sort_values(by=['difference'])
#check_ncso_list_df.head()
check_ncso_list_df.to_csv(index=False)

ncso_sum_df=ncso_df.groupby(['month',])[['actual_cost','predicted_actual_cost']].sum()  #group data to show total per month
#ncso_sum_df['predicted_actual_cost'] = ncso_sum_df['predicted_NIC'] * 0.928
ncso_sum_df['difference'] = ncso_sum_df['predicted_actual_cost'] - ncso_sum_df['actual_cost']  #calculate difference between predicted and actual
ncso_sum_df['perc_difference'] = ncso_sum_df['difference'] / ncso_sum_df['actual_cost'] #calculate percentage difference
ncso_sum_df.sort_values(by=['month']) #sort values by month for chart

ncso_sum_df.head()

ncso_sum_df.reset_index(inplace=True)

ax = ncso_sum_df.plot.bar(figsize = (12,6), y= ['perc_difference'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

ncso_sum_df['perc_difference'].std()

ncso_df.head()

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
bankhols = pd.json_normalize(bh.iloc[0]["events"])
bankhols.head()

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
dates["workdays"] = np.busday_count(begindates, enddates, weekmask = 'Mon Tue Wed Thu Fri', holidays=bankhols["date"].values.astype('datetime64[D]')) #Mon-Fri, excluding bank holidays
dates["dispdays"] = np.busday_count(begindates, enddates, weekmask = 'Mon Tue Wed Thu Fri Sat', holidays=bankhols["date"].values.astype('datetime64[D]'))#Mon-Sat, excluding bank holidays
dates.set_index('rx_month')

ncso_sum_df.reset_index()

ncso_days_sum_df = pd.merge(ncso_sum_df, dates, on='rx_month')

dates.index = pd.to_datetime(dates.index)

ncso_days_sum_df.head()

#dates['pred_month'] = dates.lookup(dates.index, dates['bdays'])
ncso_days_sum_df['dispdaysshift']=ncso_days_sum_df['dispdays'].shift(2)

ncso_days_sum_df.head(200)

ncso_days_sum_df['predicted_cost_work_days_adj']=(ncso_days_sum_df['dispdays']/ncso_days_sum_df['dispdaysshift'])*ncso_days_sum_df['predicted_actual_cost']
ncso_days_sum_df['difference_work_day_adj']=ncso_days_sum_df['predicted_cost_work_days_adj']-ncso_days_sum_df['actual_cost']
ncso_days_sum_df['percent_difference_work_days_adj']=ncso_days_sum_df['difference_work_day_adj']/ncso_days_sum_df['actual_cost']
ncso_sum_df.reset_index(inplace=True)

ncso_days_sum_df.head(200)

ax = ncso_days_sum_df.plot.bar(figsize = (12,6), y= ['percent_difference_work_days_adj'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_days_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

ncso_days_sum_df['percent_difference_work_days_adj'].std()

ncso_sum_df.head(25)

# adding in better discount

discount_df = pd.read_csv('../data/bsa_nadp.txt', engine = 'python')
discount_df['rx_month'] = discount_df['rx_month'].astype('datetime64[ns]')

discount_df.head()

ncso_discount_sum_df = pd.merge(ncso_days_sum_df, discount_df, on='rx_month')

ncso_discount_sum_df.head(200)

ncso_discount_sum_df['NADP_shift']=ncso_discount_sum_df['NADP'].shift(2)

ncso_discount_sum_df.head(200)

ncso_discount_sum_df['predicted_cost_work_days_adj_nadp']=(ncso_discount_sum_df['dispdays']/ncso_discount_sum_df['dispdaysshift'])*ncso_discount_sum_df['predicted_NIC']*(1-(ncso_discount_sum_df['NADP_shift']/100))
ncso_discount_sum_df['difference_work_day_adj_nadp']=ncso_discount_sum_df['predicted_cost_work_days_adj_nadp']-ncso_discount_sum_df['actual_cost']
ncso_discount_sum_df['percent_difference_work_days_adj_nadp']=ncso_discount_sum_df['difference_work_day_adj_nadp']/ncso_discount_sum_df['actual_cost']

ncso_discount_sum_df.head(40)

ax = ncso_discount_sum_df.plot.bar(figsize = (12,6), y= ['percent_difference_work_days_adj_nadp'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_discount_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

ncso_discount_sum_df['perc_difference'].std()

# add in average item weighting
#

# +
sql = """
select extract (month from rx.month) as mon, sum(rx.items /total_rx.total_items)/(1/12) as proportion 
from hscic.normalised_prescribing as rx,  (SELECT sum(items) as total_items FROM hscic.normalised_prescribing where month between'2017-03-01' AND '2020-02-01' and substr(bnf_code,0,2) IN ('01','02','03','04','06','10'))AS total_rx
where month between'2017-03-01' AND '2020-02-01' and substr(bnf_code,0,2) IN ('01','02','03','04','06','10')
group by mon
"""

exportfile = os.path.join("..","data","annual_profile_df.csv")
annual_profile_df = bq.cached_read(sql, csv_path=exportfile, use_cache=False)
# -

annual_profile_df.head(13)

ncso_profile_sum_new2_df = pd.merge(ncso_discount_sum_df, annual_profile_df,  on='mon')

ncso_profile_sum_new2_df.head(13)

ncso_profile_sum_new2_df['predicted_cost_work_days_adj_prop']=ncso_profile_sum_new2_df['predicted_cost_work_days_adj_nadp']*ncso_profile_sum_new2_df['proportion']
ncso_profile_sum_new2_df['difference_work_day_adj_prop']=ncso_profile_sum_new2_df['predicted_cost_work_days_adj_prop']-ncso_profile_sum_new2_df['actual_cost']
ncso_profile_sum_new2_df['percent_difference_work_days_adj_prop']=ncso_profile_sum_new2_df['difference_work_day_adj_prop']/ncso_profile_sum_new2_df['actual_cost']

ax = ncso_profile_sum_new2_df.plot.bar(figsize = (12,6), y= ['percent_difference_work_days_adj_prop'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_discount_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

ncso_profile_sum_new2_df.head()

ncso_sum_df.head()

# +
ncso_disc_sum_df = pd.merge(ncso_sum_df, discount_df, on='rx_month')
ncso_disc_sum_df=ncso_disc_sum_df.drop(columns=['quantity','rolling_ave_quantity', 'predicted_actual_cost','difference','perc_difference'])
# -


ncso_disc_sum_df['predicted_act_cost_nadp']=ncso_disc_sum_df['predicted_NIC']-(ncso_disc_sum_df['predicted_NIC']*(ncso_disc_sum_df['NADP']/100))
ncso_disc_sum_df['difference_act_cost_nadp']=ncso_disc_sum_df['predicted_act_cost_nadp']-ncso_disc_sum_df['actual_cost']
ncso_disc_sum_df['percent_predicted_act_cost_nadp']=ncso_disc_sum_df['difference_act_cost_nadp']/ncso_disc_sum_df['actual_cost']
ncso_disc_sum_df['mon'] = pd.DatetimeIndex(ncso_disc_sum_df['rx_month']).month
ncso_disc_sum_df.head()

ax = ncso_disc_sum_df.plot.bar(figsize = (12,6), y= ['percent_predicted_act_cost_nadp'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

ncso_disc_sum_df = pd.merge(ncso_disc_sum_df, annual_profile_df,  on='mon')

ncso_disc_sum_df.head()

ncso_disc_sum_df['predicted_actual_cost_prop']=ncso_disc_sum_df['predicted_act_cost_nadp']*ncso_disc_sum_df['proportion']
ncso_disc_sum_df['difference_predicted_actual_cost_prop']=ncso_disc_sum_df['predicted_actual_cost_prop']-ncso_disc_sum_df['actual_cost']
ncso_disc_sum_df['percent_predicted_actual_cost_prop']=ncso_disc_sum_df['difference_predicted_actual_cost_prop']/ncso_disc_sum_df['actual_cost']

ncso_disc_sum_df.head()

ax = ncso_disc_sum_df.plot.bar(figsize = (12,6), y= ['percent_predicted_actual_cost_prop'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['rx_month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

ncso_disc_sum_df['percent_predicted_actual_cost_prop'].std()








