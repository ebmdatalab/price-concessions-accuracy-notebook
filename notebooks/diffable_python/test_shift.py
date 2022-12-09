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

# # How accurate is the OpenPrescribing Price Concession analysis, and can we improve it?

# Price Concessions occur when pharmacies are unable to buy stock for the price listed in the Drug Tariff. These higher prices are usually due to stock availability issues.
#
# Currently the PSNC and Department of Health agree on a "price concession" at points during the month where the items have been dispensed.  This means that people are not able to find out the increased cost before dispensing.
#
# OpenPrescribing has a tool which allows an estimate of additional costs to be presented (and emailed) to users, based on a number of assumptions:
#
# - As the prescribing data is not available for the period, we use the data which is nearest available, which is usually two months beforehand.
# - We assume the national average percentage discount is 7.2%
#
# As the impact of price concessions are increasing, we thought it was time to undertake an analysis to see if our forecasting methodology was accurate enough for our users.
#

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

# We need to import data from BigQuery to undertake the analysis.
#
# One of the issues with estimating the costs of price concessions is that the concession is at an individual pack size (or `VMPP`) level, whereas prescribing data is at presentation level, and therefore may have multiple pack sizes involved.  The SQL below includes a process to only select one pack size, and if there is a difference in the cost per unit, selects the one with the highest impact on spend.
#
#

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
  rx_old.quantity AS quantity_2_months_previously ,
  rx.nic,
  rx.actual_cost,
  dt_price_pence/(100*qtyval) AS normal_nic_per_unit, --calculates "normal" drug tariff price per unit
  pc_price_pence/(100*qtyval) AS predicted_nic_per_unit -- calculates price concession predicted cost per unit
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
  DATE(rx.month) = ncso.month
  AND rx.bnf_code = ncso.bnf_code
WHERE rx.month >='2017-02-01'
ORDER BY
  rx.month
"""

exportfile = os.path.join("..","data","ncso_df.csv") #defines name for cache file
ncso_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True) #uses BQ if changed, otherwise csv cache file
ncso_df['month'] = ncso_df['month'].astype('datetime64[ns]') #ensure dates are in datetimeformat
ncso_df['normal_nic_per_unit'] = ncso_df['normal_nic_per_unit'].astype(float) #ensure in float format
ncso_df['predicted_nic_per_unit'] = ncso_df['predicted_nic_per_unit'].astype(float) #ensure in float format
# -

# Using the data imported, we can calculate the estimated impact of price concessions, using the same methodology that OpenPrescribing.net uses for initial predictions:
# - Using a fixed 7.2% [Average National Discount Percentage (NADP)](https://digital.nhs.uk/data-and-information/areas-of-interest/prescribing/practice-level-prescribing-in-england-a-summary/practice-level-prescribing-glossary-of-terms#actual-cost)
# - Using the latest data available at the time of estimate (usually two months behind)
#
# We calculate the costs by multiplying the unit quantity dispensed two months previously by the predicted cost per unit geenerated in the SQL above * 0.928.  This gives us the predicted actual cost, which we then compare to the actual amount spend in that month, and calculate the difference.

#calculate predicted costs for each drug
ncso_df['predicted_actual_cost'] = ncso_df['quantity_2_months_previously'] * ncso_df['predicted_nic_per_unit'] * 0.928 #calculate predicted actual cost - multiply by 0.928 to get actual cost, using 2 months earlier quantity data as a prediction
ncso_df['prediction_difference'] = ncso_df['actual_cost'] - ncso_df['predicted_actual_cost'] #calculate difference in costs

#create total monthly data
ncso_sum_df=ncso_df.groupby(['month',])[['actual_cost','predicted_actual_cost', 'prediction_difference']].sum()  #group data to show total per month
ncso_sum_df['perc_difference'] = ncso_sum_df['prediction_difference'] / ncso_sum_df['actual_cost'] #calculate percentage difference
ncso_sum_df.sort_values(by=['month']) #sort values by month for chart
ncso_sum_df.reset_index(inplace=True)

#create chart
ax = ncso_sum_df.plot.bar(figsize = (12,6), y= ['perc_difference'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend')

# As we can see from the chart above, on a monthly basis the price concession data is usually accurate to within 5%.  The tool usually *overestimates* (i.e. a negative percentage) in February of each year, due to the difference in working or dispensing days between the actual month and the month used for prediction (December).  

# ### Impact within Financial Year

# As the tool is mainly used to estimate the impact on finances within the NHS due to price concessions, it is also useful to see how accurate the tool is over the whole of a financial year.  Aggregating the data over a financial year is also a useful way of describe the average accuracy, which will always be flucuating on a monthly basis.

#create financial year grouping
ncso_fy_df = ncso_sum_df.groupby([pd.Grouper(key='month', freq="A-MAR")])[["actual_cost","predicted_actual_cost","prediction_difference"]].sum() #groups by financial year
ncso_fy_df['perc_difference'] = ncso_fy_df['prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference 
ncso_fy_df.reset_index(inplace=True)
ncso_fy_df = ncso_fy_df.loc[ncso_fy_df["month"].between("2017-04-01", "2022-03-31")]
ncso_fy_df = ncso_fy_df.reset_index(drop=True)

ncso_fy_df.style

#create financial year group 
ax = ncso_fy_df.plot.bar(figsize = (12,6),  y= ['perc_difference'], legend=None)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_fy_df['month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_xlabel("Financial Year ending")
ax.set_title('Percentage difference between forecasted price concession costs and actual spend (financial year)')

# As can be seen from the above, in four out of the five previous financial years the prediction tool correctly estimated within 2%.  The outlying year was 2019-2020, where the underestimation was likely to be due to the significant increase in items in March 2020 due to the onset of the coronavirus pandemic.

# ### Can we improve accuracy?

# Although it would appear that the price concession tool is accurate to usually within 2%, as the predictions are less accurate on a monthly basis, are we able to improve this?

# #### Use monthly National Average Discount Percentage

# When the price concessions tool was built a few years ago, we decided to use the NADP that was available at the time (7.2%).  Since then it has fluctuated, and the monthly value (back to 2017) is published on the [NHS BSA website]('https://www.nhsbsa.nhs.uk/prescription-data/understanding-our-data/financial-forecasting').  We can therefore import the data and adjust the prediction calculations accordingly.  We do this in the data below by dividing 7.2% by the actual NADP value, creating a weighting value to adjust the predicted actual cost calculated above.

#import NADP data (to Aug 2022)
importfile = os.path.join("..","data","nadp_fixed.csv") #define the name of the NADP import file
nadp_df = pd.read_csv(importfile) #import NADP
nadp_df['month'] = nadp_df['month'].astype('datetime64[ns]') #ensure correct date format
nadp_df['nadp_weighting'] = (1-(nadp_df['nadp']/100))/0.928 #create weighting of "true" weighting for month vs assumed 7.2%
ncso_fy_df.reset_index(inplace=True)

ncso_sum_df =  ncso_sum_df.merge(nadp_df[["month", "nadp_weighting"]]) #add weighting to grouped price concession data

# #### Weight for difference in days between prediction and actual months

# As shown above, there are often larger differences between the predicted and actual cost in months that have the most different days, with February being the most obvious.  We can try and weight to change this, by looking at *work days* (Monday-Friday), *dispensing days* (Monday-Saturday), both excluding bank holidays, and work days *including* bank holidays (as patients will tend to pick up prescriptions anyway around Christmas and Easter).  We calculate the number of working and dispensing days below, and apply a weighting to adjust the predicted actual cost.

#import bank holiday data from gov.uk and pass to busdays function `holidays=[]`
url = 'https://www.gov.uk/bank-holidays.json'
bh = pd.read_json(url, orient='index')
bankhols = pd.json_normalize(bh.iloc[0]["events"]) #flattening json in pandas df
#calculate the number of working days (Mon-Fri) and dispensing days (Mon-Sat), excluding bank holidays
import calendar
dates = ncso_df[["month"]].drop_duplicates() # find data from price concession data
dates["month"] = pd.to_datetime(dates["month"])
dates["year"] = dates["month"].dt.year
dates["mon"] = dates["month"].dt.month
d = []
for row in dates.itertuples():
    y = row.year
    m = row.mon
    day = calendar.monthrange(y,m)[1]
    d.append(str(y)+"-"+str(m)+"-"+str(day))
d = pd.Series(d, name="enddates")
d = pd.to_datetime(d, format="%Y/%m/%d")
begindates = pd.Series(dates["month"]).values.astype('datetime64[D]')
enddates = pd.Series(d).values.astype('datetime64[D]') + 1 #busday_count function doesn't include the end day, so you have to add one day to the series.
#######
# find business days in month
dates["workdays"] = np.busday_count(begindates, enddates, weekmask = 'Mon Tue Wed Thu Fri', holidays=bankhols["date"].values.astype('datetime64[D]')) #Mon-Fri, excluding bank holidays
dates["nobhworkdays"] =np.busday_count(begindates, enddates, weekmask = 'Mon Tue Wed Thu Fri') #Mon-Fri, including bank holidays
dates["dispdays"] = np.busday_count(begindates, enddates, weekmask = 'Mon Tue Wed Thu Fri Sat', holidays=bankhols["date"].values.astype('datetime64[D]'))#Mon-Sat, excluding bank holidays
#dates = dates.set_index(pd.DatetimeIndex(dates['month']))
dates['workdays_predict_weighting'] = dates['workdays']/dates['workdays'].shift(2) #calculate weighting to apply for workdays, comparing actual month with data used from two months previously
dates['nobhworkdays_predict_weighting'] = dates['nobhworkdays']/dates['nobhworkdays'].shift(2) #calculate weighting to apply for workdays, comparing actual month with data used from two months previously
dates['dispdays_predict_weighting'] = dates['dispdays']/dates['dispdays'].shift(2) #calculate weighting to apply for dispensing days, comparing actual month with data used from two months previously
#dates = dates.set_index('month')
dates = dates.sort_values(by=['month']) #sort values by month for chart

dates['month'].shift(2)

# We can also weight the effect that number of days in a month has by looking at the number of items prescribed in each month in six major chapters of the BNF, and see how it changes throughout the year, using the methodology below.  We are using five years worth of data, ending in February 2020, as the pandemic affected the number of items prescribed per month from March 2020 onwards.

# +
#calculate average proportion of prescriptions per monnth in major rx chapters
sql = """
SELECT
  EXTRACT (month
  FROM
    rx.month) AS mon, #create month of the year only
  SUM(rx.items /total_rx.total_items)/(1/12) AS proportion #calculate the relative number of prescriptions dispensed in a month, compared with fixed one-twelth
FROM
  hscic.normalised_prescribing AS rx,
  (
  SELECT
    SUM(items) AS total_items
  FROM
    hscic.normalised_prescribing
  WHERE
    month BETWEEN'2016-03-01'
    AND '2020-02-01'
    AND SUBSTR(bnf_code,0,2) IN ('01',
      '02',
      '03',
      '04',
      '06',
      '10'))AS total_rx
WHERE
  month BETWEEN'2016-03-01'
  AND '2020-02-01'
  AND SUBSTR(bnf_code,0,2) IN ('01',
    '02',
    '03',
    '04',
    '06',
    '10')
GROUP BY
  mon
"""

exportfile = os.path.join("..","data","annual_profile_df.csv")
annual_profile_df = bq.cached_read(sql, csv_path=exportfile, use_cache=False)
# -

#add the profile data to the existing date dataframe
dates = pd.merge(dates, annual_profile_df, on=["mon"]) #merge the profile data into the dates df
dates = dates.set_index('month')
dates = dates.sort_values(by=['month']) #sort values by month for chart in order to allow calculation of weighting from 2 months earlier
dates['profile_weighting'] = dates['proportion']/dates['proportion'].shift(2) # calculate difference in profile proportions
dates.reset_index(inplace=True)

# ### Assessing different weightings on accuracy of price concessions

# First we need to add the date weighting data to the price concession data

ncso_sum_df =  ncso_sum_df.merge(dates[["month", "workdays_predict_weighting","nobhworkdays_predict_weighting","dispdays_predict_weighting","profile_weighting"]]) #add weighting to grouped price concession data
ncso_sum_df.head() #show updated dataframe

# We now need to see whether the updated NADP improves the prediction further, by calculating the impact of the NADP weighting:

ncso_sum_df["nadp_predicted_actual_cost"] = ncso_sum_df["predicted_actual_cost"] * ncso_sum_df["nadp_weighting"] # calculate the predicted actual cost using NADP weighting
ncso_sum_df["nadp_prediction_difference"] = ncso_sum_df["nadp_predicted_actual_cost"] - ncso_sum_df["actual_cost"] # calculate #difference using NADP
ncso_sum_df['nadp_perc_difference'] = ncso_sum_df['nadp_prediction_difference'] / ncso_sum_df['actual_cost'] #calculate percentage difference using NADP
ncso_sum_df.reset_index(drop=True)

#create chart
ax = ncso_sum_df.plot.bar(figsize = (12,6), y= ['perc_difference', 'nadp_perc_difference'], legend=True)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_sum_df['month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_title('Percentage difference between forecasted price concession costs and actual spend, using NAPD')

#create financial year grouping
ncso_fy_df = ncso_sum_df.groupby([pd.Grouper(key='month', freq="A-MAR")])[["actual_cost","predicted_actual_cost","prediction_difference", "nadp_predicted_actual_cost","nadp_prediction_difference"]].sum() #groups by financial year
ncso_fy_df['perc_difference'] = ncso_fy_df['prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference
ncso_fy_df['nadp_perc_difference'] = ncso_fy_df['nadp_prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference with NAPD
ncso_fy_df.reset_index(inplace=True)
ncso_fy_df = ncso_fy_df.loc[ncso_fy_df["month"].between("2017-04-01", "2022-03-31")]
ncso_fy_df = ncso_fy_df.reset_index(drop=True)

#create financial year group 
ax = ncso_fy_df.plot.bar(figsize = (12,6),  y= ['perc_difference','nadp_perc_difference'], legend=True)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_fy_df['month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_xlabel("Financial Year ending")
ax.set_title('Percentage difference between forecasted price concession costs and actual spend (financial year) NADP')

# We can see from the graph above that there is a *slight* improvement for most financial years by adding in an adjustment for correct NADP, rather than the fixed value we currently use.

# We can now calculate how the different weightings for adjusting for days in the month affect the accuracy of the prediction, using the monthly NADP:

# +
ncso_sum_df["profile_predicted_actual_cost"] = ncso_sum_df["predicted_actual_cost"] * ncso_sum_df["profile_weighting"]
ncso_sum_df["profile_prediction_difference"] = ncso_sum_df["profile_predicted_actual_cost"] - ncso_sum_df["actual_cost"]
ncso_sum_df["perc_profile_prediction_difference"] = ncso_sum_df["profile_prediction_difference"] / ncso_sum_df["actual_cost"]

ncso_sum_df["profile_nadp_predicted_actual_cost"] = ncso_sum_df["predicted_actual_cost"] * ncso_sum_df["nadp_weighting"] * ncso_sum_df["profile_weighting"]
ncso_sum_df["profile_nadp_prediction_difference"] = ncso_sum_df["profile_nadp_predicted_actual_cost"] - ncso_sum_df["actual_cost"]
ncso_sum_df["perc_profile_nadp_prediction_difference"] = ncso_sum_df["profile_nadp_prediction_difference"] / ncso_sum_df["actual_cost"]

ncso_sum_df["dispdays_nadp_predicted_actual_cost"] = ncso_sum_df["predicted_actual_cost"] * ncso_sum_df["nadp_weighting"] * ncso_sum_df["dispdays_predict_weighting"]
ncso_sum_df["dispdays_nadp_prediction_difference"] = ncso_sum_df["dispdays_nadp_predicted_actual_cost"] - ncso_sum_df["actual_cost"]
ncso_sum_df["perc_dispdays_nadp_prediction_difference"] = ncso_sum_df["dispdays_nadp_prediction_difference"] / ncso_sum_df["actual_cost"]

ncso_sum_df["workdays_nadp_predicted_actual_cost"] = ncso_sum_df["predicted_actual_cost"] * ncso_sum_df["nadp_weighting"] * ncso_sum_df["workdays_predict_weighting"]
ncso_sum_df["workdays_nadp_prediction_difference"] = ncso_sum_df["workdays_nadp_predicted_actual_cost"] - ncso_sum_df["actual_cost"]
ncso_sum_df["perc_workdays_nadp_prediction_difference"] = ncso_sum_df["workdays_nadp_prediction_difference"] / ncso_sum_df["actual_cost"]

ncso_sum_df["nobhworkdays_nadp_predicted_actual_cost"] = ncso_sum_df["predicted_actual_cost"] * ncso_sum_df["nadp_weighting"] * ncso_sum_df["nobhworkdays_predict_weighting"]
ncso_sum_df["nobhworkdays_nadp_prediction_difference"] = ncso_sum_df["nobhworkdays_nadp_predicted_actual_cost"] - ncso_sum_df["actual_cost"]
ncso_sum_df["perc_nobhworkdays_nadp_prediction_difference"] = ncso_sum_df["nobhworkdays_nadp_prediction_difference"] / ncso_sum_df["actual_cost"]
# -
#create financial year grouping
ncso_fy_df = ncso_sum_df.groupby([pd.Grouper(key='month', freq="A-MAR")])[["actual_cost","predicted_actual_cost","prediction_difference","nadp_predicted_actual_cost","profile_predicted_actual_cost","profile_prediction_difference","profile_nadp_predicted_actual_cost","profile_nadp_prediction_difference","dispdays_nadp_predicted_actual_cost","dispdays_nadp_prediction_difference","workdays_nadp_predicted_actual_cost","workdays_nadp_prediction_difference","nobhworkdays_nadp_predicted_actual_cost","nobhworkdays_nadp_prediction_difference"]].sum() #groups by financial year
ncso_fy_df['perc_difference'] = ncso_fy_df['prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference
ncso_fy_df['perc_difference_profile'] = ncso_fy_df['profile_prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference 
ncso_fy_df['perc_difference_nadp_profile'] = ncso_fy_df['profile_nadp_prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference
ncso_fy_df['perc_difference_nadp_dispdays'] = ncso_fy_df['dispdays_nadp_prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference 
ncso_fy_df['perc_difference_nadp_workdays'] = ncso_fy_df['workdays_nadp_prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference 
ncso_fy_df['perc_difference_nadp_nobhworkdays'] = ncso_fy_df['nobhworkdays_nadp_prediction_difference'] / ncso_fy_df['actual_cost'] #recalculate percentage difference 
ncso_fy_df.reset_index(inplace=True)
ncso_fy_df = ncso_fy_df.loc[ncso_fy_df["month"].between("2017-04-01", "2022-03-31")]
ncso_fy_df = ncso_fy_df.reset_index(drop=True)

#create financial year group 
ax = ncso_fy_df.plot.bar(figsize = (12,6), y= ['perc_difference','perc_difference_profile','perc_difference_nadp_profile', 'perc_difference_nadp_dispdays','perc_difference_nadp_workdays','perc_difference_nadp_nobhworkdays'], legend=True)
ax.xaxis.set_major_formatter(plt.FixedFormatter(ncso_fy_df['month'].dt.strftime("%b %Y"))) #this formats date as string in desired format for x axis, formats here: https://www.ibm.com/support/knowledgecenter/SS6V3G_5.3.1/com.ibm.help.gswapplintug.doc/GSW_strdate.html
ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=None)) ##sets y axis labels as percent (and formats correctly i.e. x100)
ax.set_xlabel("Financial Year ending")
ax.set_title('Percentage difference between forecasted price concession costs and actual spend (financial year)\n using different methodologies')

# As you can see above, adjusting for number of days in a month has a minimal impact on the accuracy of the prediction tool.  It appears that using both the NADP profile and the number of working days in the month has the closest prediction.  Financial Year 2019-2020 should be excluded due to the impact of the pandemic in March 2020.

# ## Conclusions

# The OpenPrescribing.net price concessions prediction tool appears to be highly accurate when using the current methodology.  For the previous 5 years this is usually within 2% in a financial year, with 2019-2020 being an outlier for well-documented reasons.
# 2% is certainly accurate enough for planning and awareness in NHS organisations, particularly when the tool is able to identify areas of increased spend as soon as the price concessions have been released on a daily basis.
#
# There is the opportunitity to _slightly_ improve the methodology, in two ways:
# - We should consider using the current monthyl NADP profile, as opposed to the 7.2% fixed value we currently use.  This could be scraped on a monthly basis from the NHSBSA website.
# - We could consider number of workdays (excluding bank holidays) to further improve the accuracy.  This may require greater redesign of the tool.
#
# It's important to note that, given the over 98% accuracy within a financial year, that both of these changes are marginal, and may not be considered a priroity at this time.
