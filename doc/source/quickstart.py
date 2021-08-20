# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Quickstart

# %%
import datetime
import json

import pandas

from blp import blp

# %%
bquery = blp.BlpQuery().start()

# %% [markdown]
# ## Excel like functionality

# %%
bquery.bdh(
    ["SPY US Equity", "TLT US Equity"],
    ["PX_LAST", "VOLUME"],
    start_date="20190101",
    end_date="20190110",
)

# %%
bquery.bdp(["CL1 Comdty"], ["NAME", "PX_LAST"])

# %%
bquery.bds("BCOM Index", "INDX_MWEIGHT")

# %% [markdown]
# ### Using overrides
#
# Various fields can be overriden. For info on what fields support what overrides, the best place to check is using
# `FLDS` from the terminal

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

# %% [markdown]
# ### Using sedols


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
# ## Troubleshooting
#
# The most common type of error are errors based on bad input to the underlying `blpapi` service. Since `blp` is simply a wrapper on top of `blpapi`, these errors will be progagated to the user. The easiest way for resolving these is to inspect the relevant error message for any obvious problems followed by contacting the Bloomberg help desk.
#
# For example, the following code will return an error since it contains an invalid ticker
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
# If you want to ignore errors, you can instantiate a parser as follows

# %%
bquery = blp.BlpQuery(parser=blp.BlpParser(raise_security_errors=False)).start()

bquery.bdh(
    ["NOT_A_TICKER US Equity", "SPY US Equity"],
    ["PX_LAST", "VOLUME"],
    start_date="20190101",
    end_date="20190110",
)

# %% [markdown]
# ## Retrieve json data

# %% [markdown]
# ### Query generation utility methods

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
# ## Support for context manager

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
# ## Stream data

# %%
with blp.BlpStream() as bs:
    bs.subscribe({"USDCAD Curncy": {"fields": ["LAST_PRICE"]}})
    n = 0
    for ev in bs.events(timeout=60):
        print(json.dumps(ev, indent=2, cls=CustomJSONEncoder))
        n += 1
        if n > 1:
            break





# %%
