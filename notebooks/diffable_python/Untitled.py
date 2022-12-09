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
from datetime import datetime

# find consecutive at least 3 month price concessions

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
  ncso.month,
  rx.bnf_name,
  ncso.bnf_code,
  rx.quantity AS quantity,
  rx_old.quantity AS quantity_2_months_previously ,
  rx.nic,
  rx.actual_cost,
  ncso.dt_price_pence/(100*ncso.qtyval) AS normal_nic_per_unit, --calculates "normal" drug tariff price per unit,
  ncso.pc_price_pence/(100*ncso.qtyval) AS predicted_nic_per_unit, -- calculates price concession predicted cost per unit
FROM
  rx_data AS rx
INNER JOIN
  rx_data AS rx_old -- data from two months previously
ON
  rx.bnf_code = rx_old.bnf_code
  AND DATE(rx.month) = DATE_ADD(DATE(rx_old.month), INTERVAL 2 month) -- join to create data from two months ago
INNER JOIN
  price_concession AS ncso
ON
  COALESCE(DATE(rx.month),(DATE(rx_old.month)))  = ncso.month
  AND COALESCE(rx.bnf_code, rx_old.bnf_code) = ncso.bnf_code

ORDER BY
  rx.month
"""
exportfile = os.path.join("..","data","ncso_test_df.csv") #defines name for cache file
ncso_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
ncso_df['month'] = ncso_df['month'].astype('datetime64[ns]') #ensure dates are in datetimeformat
ncso_df['normal_nic_per_unit'] = ncso_df['normal_nic_per_unit'].astype(float) #ensure in float format
ncso_df['predicted_nic_per_unit'] = ncso_df['predicted_nic_per_unit'].astype(float) #ensure in float format
#ncso_df.set_index("month")


ncso_df.head()

df2 = ncso_df.groupby(['month'])['bnf_code'].count().reset_index()

first_day_of_month = pd.to_datetime(datetime.today().date().replace(day=1))
ncso_df = ncso_df.loc[(ncso_df['month'] < first_day_of_month)]
ncso_df.groupby(['month'])['bnf_code'].count().plot()

ncso_df["excess_cost_pred"] = ncso_df["quantity_2_months_previously"] * (ncso_df["predicted_nic_per_unit"] - ncso_df["normal_nic_per_unit"])

ncso_df.head()

ncso_df.groupby(['month'])['excess_cost_pred'].sum().plot()

test_df = ncso_df.groupby(['month'])['excess_cost_pred'].sum()

test_df.head(2000)







sql = """
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
    vmpp.bnf_code
"""
exportfile = os.path.join("..","data","price_df.csv") #defines name for cache file
price_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
price_df['month'] = price_df['month'].astype('datetime64[ns]') #ensure dates are in datetimeformat
price_df['increased_ppu'] = price_df['increased_ppu'].astype(float) #ensure in float formatincreased_ppu

price_df.head()

sql = """
WITH concession_list AS (
SELECT DISTINCT
    vmpp.bnf_code AS bnf_code --BNF code from VMPP table
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
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ncso.date, vmpp.bnf_code ORDER BY (ncso.price_pence - dt.price_pence)/qtyval DESC) = 1 -- for each bnf_code and pack size, calculates PPU difference and ranks in order. Takes the top value, therefore only keeping the highest impact pack size, and thereby removes duplicates for pack size)
)
SELECT
  rx.month,
  rx.bnf_name,
  rx.bnf_code,
  SUM(rx.quantity) AS quantity,
  SUM(rx.net_cost) AS nic,
  SUM(rx.actual_cost) AS actual_cost
  FROM hscic.normalised_prescribing as rx
INNER JOIN
  concession_list AS ncso
ON
rx.bnf_code = ncso.bnf_code
GROUP BY
rx.month, 
rx.bnf_name, 
rx.bnf_code
ORDER BY
  rx.month
  """
exportfile = os.path.join("..","data","rx_df.csv") #defines name for cache file
rx_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
rx_df['month'] = price_df['month'].astype('datetime64[ns]') #ensure dates are in datetimeformat

rx_df.head()

#merged_left = pd.merge(left=price_df, right=rx_df, how='left', left_on=['month', 'bnf_code'], right_on=['month', 'bnf_code'])
merged_data= price_df.merge(rx_df,how='left', on=["month", "bnf_code"])


merged_data.head(500)

merged_data.head(500)
first_day_of_month = first_day_of_month = pd.to_datetime(datetime.today().date().replace(day=1))
test = merged_data.loc[(merged_data['month'] == first_day_of_month)]

test.head(500)

quantity_df = rx_df[['month','bnf_code','quantity']].copy()

quantity_df.head()

price_df.head()


merged_data= price_df.merge(quantity_df,how='outer', on=["month", "bnf_code"])

merged_data.head()

first_day_of_month = first_day_of_month = pd.to_datetime(datetime.today().date().replace(day=1))
test = merged_data.loc[(merged_data['month'] == first_day_of_month)]

test.head()

#merged_data= price_df.merge(quantity_df,how='outer', on=["month", "bnf_code"])
df_merge_asof = pd.merge_asof(merged_data, quantity_df,
              on='month',
              by='bnf_code')


