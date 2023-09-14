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

import pandas as pd
import lxml
url = 'https://www.nhsbsa.nhs.uk/prescription-data/understanding-our-data/financial-forecasting'
dfs = pd.read_html(url,match='National Average Discount Percentage')
nadp = []
for i in range(len(dfs)):
    nadp.append(dfs[i])
nadp = pd.concat(nadp)


print(nadp)
