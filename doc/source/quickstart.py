# %%
""" [Quarto YAML Header]
---
title: "Quickstart"
theme: flatly
format:
  html:
    toc: true
    self-contained: true
    code-tools:
      source: true
      toggle: true
---
"""

# %% [markdown]
# # Introduction
# 
# We highly advise thoroughly reviewing the [BLPAPI Core Developer Guide](https://data.bloomberglp.com/professional/sites/10/2017/03/BLPAPI-Core-Developer-Guide.pdf) when developing your Bloomberg Queries. This guide is essential for understanding function parameters, request options, overrides, error messages, and troubleshooting methods.

# %%
import datetime
import json

import pandas

from blp import blp

# %%
bquery = blp.BlpQuery().start()

# %% [markdown]
# # Bloomberg Queries
# 
# ## Data History (BDH)

# %%
bquery.bdh(
    ["SPY US Equity", "TLT US Equity"],
    ["PX_LAST", "VOLUME"],
    start_date="20190101",
    end_date="20190110",
    options={"adjustmentSplit": True}
)

# %% [markdown]
# ## Data Point (BDP)

# %%
bquery.bdp(["CL1 Comdty"], ["NAME", "PX_LAST"])

# %% [markdown]
# ## Data Set (BDS)

# %%
bquery.bds("BCOM Index", "INDX_MWEIGHT")

# %% [markdown]
# ## Intraday Bar (BDIB)

# %%
bquery.bdib(
    "AAPL US Equity",
    event_type="TRADE",
    interval=60,
    start_datetime="2023-11-01", # Different date format
    end_datetime="2023-11-02", # Different date format
)

# %% [markdown]
# ## Equity Screening (BEQS)

# %%
bquery.beqs(
    "Core Capital Ratios",
    screen_type="GLOBAL",
    options={"Group": "General"}
)

# %% [markdown]
# ## BQL Queries
# 
# `bquery.bql` accepts a Bloomberg Query String. For optimal query creation, utilize Excel's BQL Builder in Advanced View. Once your query is ready, click the "Copy" button to copy it, and then paste it into the `bquery.bql` argument.

# %%
result = bquery.bql("get(px_last) for(['IBM US Equity', 'AAPL US Equity'])")
# bquery.bql returns a list of dataframes with one dataframe per field.
result[0]

# %% [markdown]
# # Advanced Query Techniques
# 
# ## Using Overrides
# 
# Various fields can be overriden. For info on what fields support what overrides, the best place to check is using `FLDS` from the terminal.

# %%
pandas.concat(
    [
        bquery.bdp(["IBM US Equity"], ["CRNCY_ADJ_PX_LAST"]),
        bquery.bdp(
            ["IBM US Equity"], ["CRNCY_ADJ_PX_LAST"], overrides=[("EQY_FUND_CRNCY", "EUR")]
        )
    ],
    axis=1,
    keys=["default", "override"]
)

# %%
bquery.bds(
    "DAX Index",
    "INDX_MWEIGHT_HIST",
    overrides=[("END_DATE_OVERRIDE", "20230630")],
)

# %% [markdown]
# ## Using SEDOLs

# %%
bquery.bdp(
    ["SPY US Equity", "TLT US Equity"],
    ["ID_SEDOL1"],
)

# %%
bquery.bdh(
    ["/sedol1/2840215", "/sedol1/2971546"],
    ["PX_LAST", "VOLUME"],
    start_date="20190101",
    end_date="20190110",
)

# %% [markdown]
# # Troubleshooting
# 
# The most frequent errors in `blpapi` are due to poor input. As `blp` is a wrapper for `blpapi`, these errors are passed to the user. To resolve them, examine the error message for issues, then contact the Bloomberg help desk.
# 
# For instance, the code below will produce an error because it includes an invalid ticker.
# 
# ```python
# bquery.bdh(
#     ["NOT_A_TICKER US Equity", "SPY US Equity"],
#     ["PX_LAST", "VOLUME"],
#     start_date="20190101",
#     end_date="20190110",
# )
# ```
# ```
# TypeError: Response for 'NOT_A_TICKER US Equity' contains securityError
# {
#     "securityError": {
#         "source": "3923::bbdbh4",
#         "code": 15,
#         "category": "BAD_SEC",
#         "message": "Unknown/Invalid securityInvalid Security [nid:3923] ",
#         "subcategory": "INVALID_SECURITY",
#     }
# }
# ```
# 
# To ignore errors, instantiate a parser as shown below:

# %%
bquery = blp.BlpQuery(parser=blp.BlpParser(raise_security_errors=False)).start()

bquery.bdh(
    ["NOT_A_TICKER US Equity", "SPY US Equity"],
    ["PX_LAST", "VOLUME"],
    start_date="20190101",
    end_date="20190110",
)

# %% [markdown]
# # Additional Features
# 
# ## Retrieve JSON Data
# 
# ### Query Generation Utility Methods

# %%
query = blp.create_query(
    request_type="HistoricalDataRequest",
    values={
        "securities": ["SPY US Equity"],
        "fields": ["VOLUME"],
        "startDate": "20190101",
        "endDate": "20190105",
    },
)
print(json.dumps(query, indent=2))

# %%
# deal with Timestamps for pretty printing for response

def ts_to_json(obj):
    return obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z")


def time_to_json(obj):
    return obj.strftime("%H:%M:%S")

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pandas.Timestamp):
            return ts_to_json(obj)
        if isinstance(obj, datetime.time):
            return time_to_json(obj)
        return super().default(obj)

# %%
resp = bquery.query(query, parse=False, collector=list)
print(json.dumps(resp, indent=2, cls=CustomJSONEncoder))

# %% [markdown]
# ## Support for Context Manager

# %%
with blp.BlpQuery() as bq:
    df = bq.bdh(
        ["GME US Equity"],
        ["PX_LAST", "VOLUME"],
        start_date="20210101",
        end_date="20210130",
    )
df

# %% [markdown]
# # Streaming Data

# %%
with blp.BlpStream() as bs:
    bs.subscribe({"USDCAD Curncy": {"fields": ["LAST_PRICE"]}})
    n = 0
    for ev in bs.events(timeout=60):
        print(json.dumps(ev, indent=2, cls=CustomJSONEncoder))
        n += 1
        if n > 1:
            break


