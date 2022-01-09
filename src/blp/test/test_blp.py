import copy
import datetime
import itertools
import queue

import blpapi
import numpy
import pandas
import pytest
import pytz
from pandas import Timestamp as TS
from pandas.testing import assert_frame_equal, assert_series_equal

from blp import blp

STREAM_SUBSCRIPTION_TIMEOUT = 20
QUERY_TIMEOUT = 50000
HOST = "localhost"
PORT = 8194

SERVICES = {
    "HistoricalDataRequest": "//blp/refdata",
    "ReferenceDataRequest": "//blp/refdata",
    "GetBrokerSpecForUuid": "//blp/emsx.brokerspec",
    "CreateOrderAndRouteEx": "//blp/emapisvc_beta",
    "GetBrokerStrategyInfoWithAssetClass": "//blp/emapisvc_beta",
}


class MockEventQueue:
    def __init__(self):
        self._values = []

    def nextEvent(self, timeout=None):
        return self._values.pop(0)

    def extend(self, value):
        self._values.extend(value)

    def __iter__(self):
        yield from self._values


class MockBlpQuery(blp.BlpQuery):
    """A mock class for :class:`blp.blp.BlpQuery`.

    Args:
        cache_data (dict): A cache of requests and responses. Keys should be the id of the request_data as it would
          appear in BlpQuery.query(), i.e. id(request_data).
          Values should be a list of responses conforming to the spec that would be yielded from get_response.

    """

    lookup = {
        "HistoricalDataRequest": "HistoricalDataResponse",
        "ReferenceDataRequest": "ReferenceDataResponse",
        "IntradayTickRequest": "IntradayTickResponse",
        "IntradayBarRequest": "IntradayBarResponse",
        "FieldInfoRequest": "FieldInfoResponse",
    }

    def __init__(
        self, host="localhost", port=8194, timeout=10000, parser=None, cache_data=None, **kwargs,
    ):
        self._cache = cache_data
        super().__init__(host=host, port=port, timeout=timeout, parser=parser, **kwargs)

    def start(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def create_request(self, request_data):
        return id(request_data)

    def send_request(self, request, data_queue, correlation_id=None):
        res = self._cache[request]
        data_queue.extend(res)

    def get_response(self, data_queue, timeout=None):
        for data in data_queue:
            yield data


@pytest.fixture(
    scope="class",
    params=[pytest.param((HOST, PORT), marks=pytest.mark.bbg)],
    ids=lambda host_port: f"{host_port[0]}:{host_port[1]}",
)
def bstream(request):
    host, port = request.param
    return blp.BlpStream(host=host, port=port, setNumStartAttempts=1)


@pytest.fixture(
    scope="module",
    params=[pytest.param((HOST, PORT), marks=pytest.mark.bbg)],
    ids=lambda host_port: f"{host_port[0]}:{host_port[1]}",
)
def bquery(request):
    host, port = request.param
    with blp.BlpQuery(host, port, timeout=QUERY_TIMEOUT) as _bquery:
        yield _bquery


@pytest.fixture(
    scope="module",
    params=[pytest.param((HOST, PORT), marks=pytest.mark.bbg)],
    ids=lambda host_port: f"{host_port[0]}:{host_port[1]}",
)
def bcon(request):
    host, port = request.param
    sopts = blpapi.SessionOptions()
    sopts.setServerHost(host)
    sopts.setServerPort(port)
    session = blpapi.Session(sopts)
    session.start()
    session.openService("//blp/refdata")
    session.openService("//blp/emsx.brokerspec")
    session.openService("//blp/emapisvc_beta")
    yield session
    session.stop()


def is_not_market_hours(now=None):
    if not now:
        now = datetime.datetime.now(tz=pytz.timezone("America/New_York"))

    friday, sunday = (4, 6)
    day_seconds = 24 * 60 * 60
    friday_close = friday * day_seconds + 17 * 60 * 60
    sunday_open = sunday * day_seconds + 18 * 60 * 60

    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_since_midnight = (now - midnight).seconds
    week_seconds = now.weekday() * 24 * 60 * 60 + seconds_since_midnight

    is_closed = (week_seconds >= friday_close) and (week_seconds <= sunday_open)
    return is_closed


def get_intraday_dates():
    prev_date = pandas.Timestamp.now() + pandas.offsets.BusinessDay(-7)
    sd = prev_date + pandas.Timedelta(hours=8)
    ed = sd + pandas.Timedelta(minutes=2)
    sd = sd.strftime("%Y-%m-%dT%H:%M:%S")
    ed = ed.strftime("%Y-%m-%dT%H:%M:%S")
    return sd, ed


def assert_streaming_equal(data, cid):
    if not isinstance(data, dict):
        raise ValueError(f"data is unknown type {type(data)}, should be dict")
    for key in data:
        if key == "fragmentType":
            assert data[key] == 0
        elif key == "correlationIds":
            assert data[key] == [cid]
        elif key == "messageType":
            assert data[key] == "MarketDataEvents"
        elif key == "topicName":
            assert data[key] == ""
        elif key == "timeReceived":
            assert isinstance(data[key], pandas.Timestamp)
        elif key == "element":
            assert list(data[key].keys()) == ["MarketDataEvents"]
        else:
            raise KeyError(f"Unknown key {key} in data")


def assert_eventdata_equal(data, exp_data):
    assert len(data) == len(exp_data)
    for d, ed in zip(data, exp_data):
        cid = d["message"].pop("correlationIds")
        ed["message"].pop("correlationIds")
        assert len(cid) == 1
        assert isinstance(cid[0], int)
    assert data == exp_data


def assert_info_equal(data, exp_data, ignore_overrides=True):
    data = copy.deepcopy(data)
    exp_data = copy.deepcopy(exp_data)
    assert len(data) == len(exp_data)
    for d, ed in zip(data, exp_data):
        cid = d["message"].pop("correlationIds")
        ed["message"].pop("correlationIds")
        assert len(cid) == 1
        assert isinstance(cid[0], int)
        d["message"]["element"]["fieldResponse"] = sorted(
            d["message"]["element"]["fieldResponse"], key=lambda x: x["fieldData"]["id"]
        )
        ed["message"]["element"]["fieldResponse"] = sorted(
            ed["message"]["element"]["fieldResponse"], key=lambda x: x["fieldData"]["id"],
        )
        if ignore_overrides:
            for di in [d, ed]:
                for resp in di["message"]["element"]["fieldResponse"]:
                    if "fieldInfo" in resp["fieldData"]:
                        del resp["fieldData"]["fieldInfo"]["fieldInfo"]["overrides"]
                    if "fieldError" in resp["fieldData"]:
                        del resp["fieldData"]["fieldError"]["fieldError"]["source"]
    assert data == exp_data


def assert_bar_parsed(data, security, event):
    for datum in data:
        assert set(datum) == {"security", "data", "events"}
        assert datum["security"] == security
        assert datum["events"] == [event]
        for bar in datum["data"]:
            assert set(bar) == {
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "numEvents",
                "value",
            }
            assert isinstance(bar["time"], pandas.Timestamp)
            assert isinstance(bar["open"], float)
            assert isinstance(bar["high"], float)
            assert isinstance(bar["low"], float)
            assert isinstance(bar["close"], float)
            assert isinstance(bar["volume"], int)
            assert isinstance(bar["numEvents"], int)
            assert isinstance(bar["value"], float)


def assert_tick_parsed(data, security, event):
    for datum in data:
        assert set(datum) == {"security", "data", "events"}
        assert datum["security"] == security
        assert datum["events"] == [event]
        for tick in datum["data"]:
            assert set(tick) == {"time", "type", "value", "size"}
            assert isinstance(tick["time"], pandas.Timestamp)
            assert tick["type"] == event
            assert isinstance(tick["value"], float)
            assert isinstance(tick["size"], int)


def params_from_funcs(funcs):
    ids = []
    argvalues = []
    for f in funcs:
        ids.append(f.__name__)
        argvalues.append(f())
    return {"ids": ids, "argvalues": argvalues}


def test_connect(bstream):
    with bstream:
        pass


def test_iterate_empty_timeout(bstream):
    with bstream:
        with pytest.raises(queue.Empty):
            for _ in bstream.events(timeout=0.1):
                pass


def test_subscribe_data(bstream):
    sub_dict = {
        "DOESCRUD Index": {"fields": ["LAST_PRICE"]},
        "BAD_TICKER": {"fields": ["LAST_PRICE"]},
        "USDCAD Curncy": {"fields": ["BAD_FIELD"]},
        "EURUSD Curncy": {"fields": ["LAST_PRICE", "BAD_FIELD"]},
    }
    res_exp = {
        "DOESCRUD Index": True,
        "BAD_TICKER": False,
        "USDCAD Curncy": False,
        "EURUSD Curncy": ["BAD_FIELD"],
    }
    with bstream:
        res = bstream.subscribe(sub_dict, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
    assert res == res_exp


def test_unsubscribe_data(bstream):
    sub_dict = {"DOESCRUD Index": {"fields": ["LAST_PRICE"]}}
    res_exp = {"DOESCRUD Index": True}
    with bstream:
        res1 = bstream.subscribe(sub_dict, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
        res2 = bstream.unsubscribe(sub_dict, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
    assert res1 == res_exp
    assert res2 == res_exp


def test_resubscribe_data(bstream):
    sub_dict = {"DOESCRUD Index": {"fields": ["LAST_PRICE"]}}
    res_exp = {"DOESCRUD Index": True}
    with bstream:
        res1 = bstream.subscribe(sub_dict, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
        res2 = bstream.resubscribe(sub_dict, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
    assert res1 == res_exp
    assert res2 == res_exp


def test_resubscribe_missing_correlationid(bstream):
    sub_dict1 = {"DOESCRUD Index": {"fields": ["LAST_PRICE"]}}
    sub_dict2 = {
        "DOESCRUD Index": {"fields": ["LAST_PRICE"]},
        "USDCAD Curncy": {"fields": ["PX_LAST"]},
    }
    with bstream:
        bstream.subscribe(sub_dict1, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
        with pytest.raises(blpapi.exception.NotFoundException):
            bstream.resubscribe(sub_dict2, timeout=STREAM_SUBSCRIPTION_TIMEOUT)


def test_unsubscribe_missing_correlationid(bstream):
    sub_dict1 = {"DOESCRUD Index": {"fields": ["LAST_PRICE"]}}
    sub_dict2 = {
        "DOESCRUD Index": {"fields": ["LAST_PRICE"]},
        "USDCAD Curncy": {"fields": ["PX_LAST"]},
    }
    with bstream:
        bstream.subscribe(sub_dict1, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
        with pytest.raises(queue.Empty):
            bstream.unsubscribe(sub_dict2, timeout=1)


@pytest.mark.skipif(is_not_market_hours(), reason="Requires market to be open")
def test_iter_data(bstream):
    sub_dict = {"USDCAD Curncy": {"fields": ["LAST_PRICE"]}}
    data = []
    max_events = 3
    with bstream:
        bstream.subscribe(sub_dict, timeout=STREAM_SUBSCRIPTION_TIMEOUT)
        # add timeout to avoid hanging indefinitely if no market data is arriving
        for datum in bstream.events(timeout=60):
            data.append(datum)
            if len(data) == max_events:
                break
    for d in data:
        assert_streaming_equal(d, "USDCAD Curncy")


def bbg_request_params():
    historical = {
        "fields": ["PX_LAST", "PX_VOLUME"],
        "securities": ["SPY US Equity", "IBM US Equity"],
        "startDate": "20180101",
        "endDate": "20180105",
    }
    historical_adjustments = {
        "fields": ["PX_LAST", "PX_VOLUME"],
        "securities": ["SPY US Equity", "IBM US Equity"],
        "startDate": "20180101",
        "endDate": "20180105",
        "periodicityAdjustment": "ACTUAL",
    }
    reference = {
        "fields": ["PX_LAST", "PX_VOLUME"],
        "securities": ["SPY US Equity", "IBM US Equity"],
    }
    reference_overrides = {
        "fields": ["SETTLE_DT"],
        "securities": ["AUD Curncy"],
        "overrides": [{"overrides": {"fieldId": "REFERENCE_DATE", "value": "20180101"}}],
    }
    broker_spec = {"uuid": 1234567}
    order_and_route = {
        "EMSX_TICKER": "IBM US Equity",
        "EMSX_AMOUNT": 1,
        "EMSX_ORDER_TYPE": "MKT",
        "EMSX_TIF": "DAY",
        "EMSX_HAND_INSTRUCTION": "ANY",
        "EMSX_SIDE": "BUY",
        "EMSX_BROKER": "BB",
    }
    broker_strategy = {
        "EMSX_REQUEST_SEQ": 1,
        "EMSX_ASSET_CLASS": "FUT",
        "EMSX_BROKER": "BMTB",
        "EMSX_STRATEGY": "VWAP",
    }

    return {
        "ids": [
            "historical",
            "historical_adjustments",
            "reference",
            "reference_overrides",
            "broker_spec",
            "order_and_route",
            "broker_strategy",
        ],
        "argvalues": [
            ("HistoricalDataRequest", historical),
            ("HistoricalDataRequest", historical_adjustments),
            ("ReferenceDataRequest", reference),
            ("ReferenceDataRequest", reference_overrides),
            ("GetBrokerSpecForUuid", broker_spec),
            ("CreateOrderAndRouteEx", order_and_route),
            ("GetBrokerStrategyInfoWithAssetClass", broker_strategy),
        ],
    }


@pytest.mark.parametrize("request_type, request_data", **bbg_request_params())
def test_dict_to_request(bcon, request_type, request_data):
    bbg_request = bcon.getService(SERVICES[request_type]).createRequest(request_type)
    round_trip = blp.element_to_dict(blp.dict_to_req(bbg_request, request_data).asElement())
    round_trip_data = round_trip[list(round_trip)[0]]
    assert request_data == round_trip_data


@pytest.mark.parametrize(
    "request_data",
    ids=["reference_empty", "reference", "historical"],
    argvalues=[
        {"ReferenceDataRequest": {}},
        {"ReferenceDataRequest": {"fields": ["PX_LAST"], "securities": ["SPY US Equity"]}},
        {
            "HistoricalDataRequest": {
                "fields": ["PX_LAST"],
                "securities": ["SPY US Equity"],
                "startDate": "20180101",
                "endDate": "20180105",
            }
        },
    ],
)
def test_create_request_smoketest(bquery, request_data):
    bquery.create_request(request_data)


def historical_response():
    return [
        {
            "eventType": 6,
            "eventTypeName": "blpapi.Event.PARTIAL_RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [1],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "SPY US Equity",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [],
                            "fieldData": [
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-02 00:00:00"),
                                        "PX_LAST": 268.77,
                                        "PX_VOLUME": 86655749.0,
                                    }
                                },
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-03 00:00:00"),
                                        "PX_LAST": 270.47,
                                        "PX_VOLUME": 90070416.0,
                                    }
                                },
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-04 00:00:00"),
                                        "PX_LAST": 271.61,
                                        "PX_VOLUME": 80636408.0,
                                    }
                                },
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-05 00:00:00"),
                                        "PX_LAST": 273.42,
                                        "PX_VOLUME": 83523995.0,
                                    }
                                },
                            ],
                        }
                    }
                },
            },
        },
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [1],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "IBM US Equity",
                            "eidData": [],
                            "sequenceNumber": 1,
                            "fieldExceptions": [],
                            "fieldData": [
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-02 00:00:00"),
                                        "PX_LAST": 154.25,
                                        "PX_VOLUME": 4202503.0,
                                    }
                                },
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-03 00:00:00"),
                                        "PX_LAST": 158.49,
                                        "PX_VOLUME": 9441567.0,
                                    }
                                },
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-04 00:00:00"),
                                        "PX_LAST": 161.7,
                                        "PX_VOLUME": 7556249.0,
                                    }
                                },
                                {
                                    "fieldData": {
                                        "date": TS("2018-01-05 00:00:00"),
                                        "PX_LAST": 162.49,
                                        "PX_VOLUME": 5195764.0,
                                    }
                                },
                            ],
                        }
                    }
                },
            },
        },
    ]


def test_get_response(bquery):
    reqd = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST", "PX_VOLUME"],
            "securities": ["SPY US Equity", "IBM US Equity"],
            "startDate": "20180101",
            "endDate": "20180105",
        }
    }
    req = bquery.create_request(reqd)
    data_queue = blpapi.EventQueue()
    bquery.send_request(req, data_queue)
    res_list = [data for data in bquery.get_response(data_queue)]
    exp_res = historical_response()
    assert_eventdata_equal(res_list, exp_res)


def test_bquery_timeout(bquery):
    with pytest.raises(ConnectionError):
        [i for i in bquery.get_response(blpapi.EventQueue(), timeout=1)]


def test_query_historical_data(bquery):
    reqd = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST", "PX_VOLUME"],
            "securities": ["SPY US Equity", "IBM US Equity"],
            "startDate": "20180101",
            "endDate": "20180105",
        }
    }
    res_list = bquery.query(reqd, parse=False, collector=list)
    exp_res = historical_response()
    assert_eventdata_equal(res_list, exp_res)


def test_query_reference_data(bquery):
    reqd = {"ReferenceDataRequest": {"fields": ["NAME"], "securities": ["AUD Curncy", "EUR Curncy"]}}
    res_list = bquery.query(reqd, parse=False, collector=list)
    exp_res = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [1],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "AUD Curncy",
                                "eidData": [],
                                "fieldExceptions": [],
                                "sequenceNumber": 0,
                                "fieldData": {"fieldData": {"NAME": "Australian Dollar Spot"}},
                            }
                        },
                        {
                            "securityData": {
                                "security": "EUR Curncy",
                                "eidData": [],
                                "fieldExceptions": [],
                                "sequenceNumber": 1,
                                "fieldData": {"fieldData": {"NAME": "Euro Spot"}},
                            }
                        },
                    ]
                },
            },
        }
    ]
    assert_eventdata_equal(res_list, exp_res)


def test_query_bulk_reference_data(bquery):
    reqd = {
        "ReferenceDataRequest": {
            "securities": ["GC1 Comdty"],
            "fields": ["FUT_CHAIN"],
            "overrides": [{"overrides": {"fieldId": "CHAIN_DATE", "value": "20050101"}}],
        }
    }
    res_list = bquery.query(reqd, parse=False, collector=list)
    exp_res = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [18],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "GC1 Comdty",
                                "eidData": [],
                                "fieldExceptions": [],
                                "sequenceNumber": 0,
                                "fieldData": {
                                    "fieldData": {
                                        "FUT_CHAIN": [
                                            {"FUT_CHAIN": {"Security Description": "GCF05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCG05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCH05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCJ05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCM05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCQ05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCV05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCZ05 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCG06 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCJ06 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCM06 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCQ06 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCV06 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCZ06 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCM07 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCZ07 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCM08 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCZ08 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCM09 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "GCZ09 Comdty"}},
                                        ]
                                    }
                                },
                            }
                        }
                    ]
                },
            },
        }
    ]
    assert_eventdata_equal(res_list, exp_res)


def field_info_one_field():
    request = {"FieldInfoRequest": {"id": ["PX_LAST"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [1],
                "messageType": "fieldResponse",
                "timeReceived": None,
                "element": {
                    "fieldResponse": [
                        {
                            "fieldData": {
                                "id": "PR005",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "PX_LAST",
                                        "description": "Last Price",
                                        "datatype": "Double",
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [
                                            "PX628",
                                            "DY628",
                                            "DZ066",
                                            "DT454",
                                            "FL021",
                                            "PX957",
                                            "FL039",
                                            "FL037",
                                            "FL026",
                                            "SP162",
                                            "DS028",
                                            "FL235",
                                            "FO009",
                                            "FL024",
                                            "DT456",
                                            "FL023",
                                            "DY719",
                                            "AN175",
                                            "PX342",
                                            "DS029",
                                            "DY630",
                                            "DY629",
                                            "DT455",
                                            "DS170",
                                            "YL112",
                                        ],
                                        "ftype": "Price",
                                    }
                                },
                            }
                        }
                    ]
                },
            },
        }
    ]
    return request, response


def field_info_one_field_with_docs():
    request = {"FieldInfoRequest": {"id": ["PX_LAST"], "returnFieldDocumentation": True}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [1],
                "messageType": "fieldResponse",
                "timeReceived": None,
                "element": {
                    "fieldResponse": [
                        {
                            "fieldData": {
                                "id": "PR005",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "PX_LAST",
                                        "description": "Last Price",
                                        "datatype": "Double",
                                        "documentation": "Last price for the security.\n\nEquities:\n  Returns the last price provided by the exchange. For securities that trade Monday through Friday, this field will be populated only if such information has been provided by the exchange in the past 30 trading days. For initial public offerings (IPO), the day before the first actual trading day may return the IPO price. For all other securities, this field will be populated only if such information was provided by the exchange in the last 30 calendar days. This applies to common stocks, receipts, warrants, and real estate investment trusts (REITs).\n\nEquity Derivatives:\n  Equity Options, Spot Indices, Index Futures and Commodity Futures:\n    Returns the last trade price. No value is returned for expired contracts.\n\n  Synthetic Options:\n    Returns N.A.\n\nFixed Income:\n  Returns the last price received from the current pricing source. The last price will always come from the date and time in LAST_UPDATE/LAST_UPDATE_DT. If there was no contributed last at that time the first valid value from mid/bid/ask will be used. The value returned will be a discount if Pricing Source Quote Type (DS962, PCS_QUOTE_TYP) is 2 (Discount Quoted). For information specific to the last trade see the price (PR088, PX_LAST_ACTUAL), time (P2788, LAST_TRADE_TIME), and date (P2789, LAST_TRADE_DATE) fields.\n\nReturns the last price received from the current pricing source. If last price is not available, then a mid computed from bid and ask will be returned. If either bid or ask is not available to compute mid, the field returns whichever side that is received.\n\nEquity Indices:\n  Returns either the current quote price of the index or the last available close price of the index.\n\nCustom Indices:\n  Returns the value the custom index expression evaluates to. Since the expression is user defined, the value has no units.\n\nEconomic Statistics:\n  Provides the revision of the prior release. \n\nFutures and Options:\n  Returns the last traded price until settlement price is received, at which time the settlement price is returned. If no trade or settlement price is available for the current day, then the last settlement price received is provided. No value is returned for expired contracts.\nSettlement Price (PR277, PX_SETTLE) and Futures Trade Price (PR083, FUT_PX) can be used instead to return settlement price and closing price respectively at all times regardless of these parameters.\n\nSwaps and Credit Default Swaps:\n  Not supported for synthetics.\n\nMutual Funds:\n  Closed-End, Exchange Traded and Open-End Funds Receiving Intraday Pricing from Exchange Feeds:\n    Returns the most recent trade price.\n\n  Open-End and Hedge Funds:\n    Returns the net asset value (NAV). If no NAV is available, the bid is returned, and if no bid is available then the ask is returned.\n\n  Money Market Funds that Display Days to Maturity and Yield:\n    Returns a yield.\n\nCurrencies:\n  Broken Date Type Currencies (e.g. USD/JPY 3M Curncy):\n    Returns the average of the bid and ask.\n\n  For All Other Currency Types:\n    Returns the last trade price if it is valid and available. If last trade is not available then mid price is returned. Mid price is the average of the bid and ask. If a valid bid and ask are not available, then a bid or ask is returned based on which is non-zero. If no data is available for the current day, then the previous day's last trade is returned.\n\nOTC FX Options:\n  Returns the premium of the option in nominal amount. Returns the price of the option expressed in a currency opposite of the notional currency.\n\nMortgages:\n  Returns the last price received from the current pricing source. If this field is empty for any reason, then last ask is returned and if no ask is available, then last bid is returned.\n\nMunicipals:\n  Returns the last price received from the current pricing source.\n\nPortfolio:\nNet asset value (NAV) as computed in the Portfolio & Risk Analytics function and used for Total Return computations. It is the cumulated daily total returns applied to the user-defined price/value at portfolio's inception date.",  # noqa 501
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [
                                            "PX628",
                                            "DY628",
                                            "DZ066",
                                            "DT454",
                                            "FL021",
                                            "PX957",
                                            "FL039",
                                            "FL037",
                                            "FL026",
                                            "SP162",
                                            "DS028",
                                            "FL235",
                                            "FO009",
                                            "FL024",
                                            "DT456",
                                            "FL023",
                                            "DY719",
                                            "AN175",
                                            "PX342",
                                            "DS029",
                                            "DY630",
                                            "DY629",
                                            "DT455",
                                            "DS170",
                                            "YL112",
                                        ],
                                        "ftype": "Price",
                                    }
                                },
                            }
                        }
                    ]
                },
            },
        }
    ]
    return request, response


def field_info_two_fields():
    request = {"FieldInfoRequest": {"id": ["PX_LAST", "NAME"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [1],
                "messageType": "fieldResponse",
                "timeReceived": None,
                "element": {
                    "fieldResponse": [
                        {
                            "fieldData": {
                                "id": "PR005",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "PX_LAST",
                                        "description": "Last Price",
                                        "datatype": "Double",
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [
                                            "PX628",
                                            "DY628",
                                            "DZ066",
                                            "DT454",
                                            "FL021",
                                            "PX957",
                                            "FL039",
                                            "FL037",
                                            "FL026",
                                            "SP162",
                                            "DS028",
                                            "FL235",
                                            "FO009",
                                            "FL024",
                                            "DT456",
                                            "FL023",
                                            "DY719",
                                            "AN175",
                                            "PX342",
                                            "DS029",
                                            "DY630",
                                            "DY629",
                                            "DT455",
                                            "DS170",
                                            "YL112",
                                        ],
                                        "ftype": "Price",
                                    }
                                },
                            }
                        },
                        {
                            "fieldData": {
                                "id": "DS002",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "NAME",
                                        "description": "Name",
                                        "datatype": "String",
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [],
                                        "ftype": "Character",
                                    }
                                },
                            }
                        },
                    ]
                },
            },
        }
    ]
    return request, response


def field_info_bad_field():
    request = {"FieldInfoRequest": {"id": ["bad_field"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [1],
                "messageType": "fieldResponse",
                "timeReceived": None,
                "element": {
                    "fieldResponse": [
                        {
                            "fieldData": {
                                "id": "BAD_FIELD",
                                "fieldError": {
                                    "fieldError": {
                                        "source": "apiflds@tkusr-ob-009",
                                        "code": -103,
                                        "category": "BAD_FLD",
                                        "message": "Unknown Field Id/Mnemonic",
                                    }
                                },
                            }
                        }
                    ]
                },
            },
        }
    ]
    return request, response


def field_info_params():
    funcs = [
        field_info_one_field,
        field_info_one_field_with_docs,
        field_info_two_fields,
        field_info_bad_field,
    ]
    return params_from_funcs(funcs)


@pytest.mark.parametrize("bbg_request, exp_res", **field_info_params())
def test_query_field_info(bquery, bbg_request, exp_res):
    res_list = bquery.query(bbg_request, parse=False, collector=list)
    assert_info_equal(res_list, exp_res)


def test_bquery_context_smoketest(bquery):
    reqd = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST"],
            "securities": ["SPY US Equity"],
            "startDate": "20180101",
            "endDate": "20180105",
        }
    }
    with blp.BlpQuery(HOST, PORT):
        bquery.query(reqd)


def parser_raises_params():
    bad_field = {
        "HistoricalDataRequest": {
            "fields": ["BAD_FIELD"],
            "securities": ["SPY US Equity"],
            "startDate": "20180101",
            "endDate": "20180105",
        }
    }
    bad_security = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST"],
            "securities": ["BAD_SECURITY"],
            "startDate": "20180101",
            "endDate": "20180105",
        }
    }
    missing_start_date = {"HistoricalDataRequest": {"fields": ["PX_LAST"], "securities": ["BAD_SECURITY"]}}
    non_applicable_field_hist = {
        "HistoricalDataRequest": {
            "fields": ["PX_VOLUME"],
            "securities": ["DOESCRUD Index"],
            "startDate": "20180102",
            "endDate": "20180203",
        }
    }
    non_applicable_field_one_valid_hist = {
        "HistoricalDataRequest": {
            "fields": ["PX_VOLUME", "PX_LAST"],
            "securities": ["DOESCRUD Index"],
            "startDate": "20180102",
            "endDate": "20180203",
        }
    }
    bad_fld_invalid_ref = {"ReferenceDataRequest": {"fields": ["not_a_field"], "securities": ["RSF82 Comdty"]}}

    return {
        "ids": [
            "bad_field",
            "bad_security",
            "missing_start_date",
            "non_applicable_field_hist",
            "non_applicable_field_one_valid_hist",
            "bad_fld_invalid_ref",
        ],
        "argvalues": [
            (bad_field, blp.BlpParser._process_field_exception),
            (bad_security, blp.BlpParser._validate_security_error),
            (missing_start_date, blp.BlpParser._validate_response_error),
            (non_applicable_field_hist, blp.BlpParser._validate_fields_exist),
            (non_applicable_field_one_valid_hist, blp.BlpParser._validate_fields_exist),
            (bad_fld_invalid_ref, blp.BlpParser._process_field_exception),
        ],
    }


@pytest.mark.parametrize("bbg_request, processor", **parser_raises_params())
def test_parser_processors_raises(bquery, bbg_request, processor):
    response = bquery.query(bbg_request, parse=False, collector=next)
    with pytest.raises(TypeError):
        processor(response, bbg_request)


def parser_valid_params():
    valid_eco_hist = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST"],
            "securities": ["DOESCRUD Index"],
            "startDate": "20180102",
            "endDate": "20180203",
        }
    }
    non_applicable_field_ref = {"ReferenceDataRequest": {"fields": ["ID_EXCH_SYMBOL"], "securities": ["RSF82 Comdty"]}}
    return {
        "ids": ["valid_eco_hist", "non_applicable_field_ref"],
        "argvalues": [
            (valid_eco_hist, blp.BlpParser._validate_fields_exist),
            (non_applicable_field_ref, blp.BlpParser._process_field_exception),
        ],
    }


@pytest.mark.parametrize("bbg_request, processor", **parser_valid_params())
def test_parser_processors_valid(bquery, bbg_request, processor):
    response = bquery.query(bbg_request, parse=False, collector=next)
    processor(response, bbg_request)


def mock_timeout_response():
    request = {"HistoricalDataRequest": {}}
    response = [{"eventType": 10, "eventTypeName": "blpapi.Event.TIMEOUT", "message": {}, "messageNumber": 0}]
    data = {id(request): response}
    return data, request, blp.BlpParser._validate_event


def mock_unknown_response():
    request = {"HistoricalDataRequest": {}}
    response = [{"eventType": -1, "eventTypeName": "blpapi.Event.UNKNOWN", "message": {}, "messageNumber": 0}]
    data = {id(request): response}
    return data, request, blp.BlpParser._validate_event


def mock_bad_field():
    request = {
        "HistoricalDataRequest": {
            "fields": ["BAD_FIELD"],
            "securities": ["SPY US Equity"],
            "startDate": "20180101",
            "endDate": "20180105",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [35],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "SPY US Equity",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [
                                {
                                    "fieldExceptions": {
                                        "fieldId": "BAD_FIELD",
                                        "errorInfo": {
                                            "errorInfo": {
                                                "source": "2979::bbdbh1",
                                                "code": 1,
                                                "category": "BAD_FLD",
                                                "message": "Invalid field",
                                                "subcategory": "NOT_APPLICABLE_TO_HIST_DATA",
                                            }
                                        },
                                    }
                                }
                            ],
                            "fieldData": [],
                        }
                    }
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._process_field_exception


def mock_bad_security():
    request = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST"],
            "securities": ["BAD_SECURITY"],
            "startDate": "20180101",
            "endDate": "20180105",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [30],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "BAD_SECURITY",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "securityError": {
                                "securityError": {
                                    "source": "2979::bbdbh4",
                                    "code": 15,
                                    "category": "BAD_SEC",
                                    "message": "Unknown/Invalid securityInvalid Security [nid:2979] ",
                                    "subcategory": "INVALID_SECURITY",
                                }
                            },
                            "fieldExceptions": [],
                            "fieldData": [],
                        }
                    }
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._validate_security_error


def mock_missing_start_date():
    request = {"HistoricalDataRequest": {"fields": ["PX_LAST"], "securities": ["BAD_SECURITY"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [25],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "responseError": {
                            "source": "bbdbh4",
                            "code": 30,
                            "category": "BAD_ARGS",
                            "message": "Invalid start date specified [nid:2979] ",
                            "subcategory": "INVALID_START_DATE",
                        }
                    }
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._validate_response_error


def mock_non_applicable_field_hist():
    request = {
        "HistoricalDataRequest": {
            "fields": ["PX_VOLUME"],
            "securities": ["DOESCRUD Index"],
            "startDate": "20180105",
            "endDate": "20180112",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [20],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "DOESCRUD Index",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [],
                            "fieldData": [],
                        }
                    }
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._validate_fields_exist


def mock_non_applicable_field_one_valid_hist():
    request = {
        "HistoricalDataRequest": {
            "fields": ["PX_VOLUME", "PX_LAST"],
            "securities": ["DOESCRUD Index"],
            "startDate": "20180105",
            "endDate": "20180112",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [15],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "DOESCRUD Index",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [],
                            "fieldData": [
                                {"fieldData": {"date": TS("2018-01-05 00:00:00"), "PX_LAST": 419515.0}},
                                {"fieldData": {"date": TS("2018-01-12 00:00:00"), "PX_LAST": 412654.0}},
                            ],
                        }
                    }
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._validate_fields_exist


def mock_bad_fld_invalid_ref():
    request = {"ReferenceDataRequest": {"fields": ["not_a_field"], "securities": ["RSF82 Comdty"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [5],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "RSF82 Comdty",
                                "eidData": [],
                                "fieldExceptions": [
                                    {
                                        "fieldExceptions": {
                                            "fieldId": "not_a_field",
                                            "errorInfo": {
                                                "errorInfo": {
                                                    "source": "180::bbdbl9",
                                                    "code": 9,
                                                    "category": "BAD_FLD",
                                                    "message": "Field not valid",
                                                    "subcategory": "INVALID_FIELD",
                                                }
                                            },
                                        }
                                    }
                                ],
                                "sequenceNumber": 0,
                                "fieldData": {"fieldData": {}},
                            }
                        }
                    ]
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._process_field_exception


def mock_parser_raises_params():
    funcs = [
        mock_timeout_response,
        mock_unknown_response,
        mock_bad_field,
        mock_bad_security,
        mock_missing_start_date,
        mock_non_applicable_field_hist,
        mock_non_applicable_field_one_valid_hist,
        mock_bad_fld_invalid_ref,
    ]
    return params_from_funcs(funcs)


@pytest.mark.parametrize("data, bbg_request, processor", **mock_parser_raises_params())
def test_mocked_parser_processors_raises(data, bbg_request, processor, mocker):
    mocker.patch("blpapi.EventQueue", MockEventQueue)
    mocker.patch("blp.blp.BlpQuery", MockBlpQuery)

    response = blp.BlpQuery(cache_data=data).query(bbg_request, parse=False, collector=next)
    with pytest.raises(TypeError):
        processor(response, bbg_request)


def mock_valid_eco_hist():
    request = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST"],
            "securities": ["DOESCRUD Index"],
            "startDate": "20180105",
            "endDate": "20180112",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [10],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "DOESCRUD Index",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [],
                            "fieldData": [
                                {"fieldData": {"date": TS("2018-01-05 00:00:00"), "PX_LAST": 419515.0}},
                                {"fieldData": {"date": TS("2018-01-12 00:00:00"), "PX_LAST": 412654.0}},
                            ],
                        }
                    }
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._validate_fields_exist


def mock_non_applicable_field_ref():
    request = {"ReferenceDataRequest": {"fields": ["ID_EXCH_SYMBOL"], "securities": ["RSF82 Comdty"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [11],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "RSF82 Comdty",
                                "eidData": [],
                                "fieldExceptions": [
                                    {
                                        "fieldExceptions": {
                                            "fieldId": "ID_EXCH_SYMBOL",
                                            "errorInfo": {
                                                "errorInfo": {
                                                    "source": "3589::bbdbd14",
                                                    "code": 9,
                                                    "category": "BAD_FLD",
                                                    "message": "Field not applicable to security",
                                                    "subcategory": "NOT_APPLICABLE_TO_REF_DATA",
                                                }
                                            },
                                        }
                                    }
                                ],
                                "sequenceNumber": 0,
                                "fieldData": {"fieldData": {}},
                            }
                        }
                    ]
                },
            },
        }
    ]
    data = {id(request): response}
    return data, request, blp.BlpParser._process_field_exception


def mock_parser_valid_params():
    funcs = [mock_valid_eco_hist, mock_non_applicable_field_ref]
    return params_from_funcs(funcs)


@pytest.mark.parametrize("data, bbg_request, processor", **mock_parser_valid_params())
def test_mocked_parser_processors_valid(data, bbg_request, processor, mocker):
    mocker.patch("blpapi.EventQueue", MockEventQueue)
    mocker.patch("blp.blp.BlpQuery", MockBlpQuery)

    response = blp.BlpQuery(cache_data=data).query(bbg_request, parse=False, collector=next)
    processor(response, bbg_request)


def parser_parse_historical():
    request = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST"],
            "securities": ["SPY US Equity"],
            "startDate": "20180102",
            "endDate": "20180103",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [15],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "SPY US Equity",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [],
                            "fieldData": [
                                {"fieldData": {"date": TS("2018-01-02 00:00:00"), "PX_LAST": 268.77}},
                                {"fieldData": {"date": TS("2018-01-03 00:00:00"), "PX_LAST": 270.47}},
                            ],
                        }
                    }
                },
            },
        }
    ]
    parsed = [
        {
            "security": "SPY US Equity",
            "fields": ["PX_LAST"],
            "data": [{"date": TS(2018, 1, 2), "PX_LAST": 268.77}, {"date": TS(2018, 1, 3), "PX_LAST": 270.47}],
        }
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_multi_historical():
    request = {
        "HistoricalDataRequest": {
            "fields": ["PX_LAST"],
            "securities": ["SPY US Equity", "TLT US Equity"],
            "startDate": "20180103",
            "endDate": "20180103",
        }
    }
    response = [
        {
            "eventType": 6,
            "eventTypeName": "blpapi.Event.PARTIAL_RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [8],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "SPY US Equity",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [],
                            "fieldData": [{"fieldData": {"date": TS("2018-01-03 00:00:00"), "PX_LAST": 270.47}}],
                        }
                    }
                },
            },
        },
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [8],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "TLT US Equity",
                            "eidData": [],
                            "sequenceNumber": 1,
                            "fieldExceptions": [],
                            "fieldData": [{"fieldData": {"date": TS("2018-01-03 00:00:00"), "PX_LAST": 126.09}}],
                        }
                    }
                },
            },
        },
    ]
    parsed = [
        {"security": "SPY US Equity", "fields": ["PX_LAST"], "data": [{"date": TS(2018, 1, 3), "PX_LAST": 270.47}]},
        {"security": "TLT US Equity", "fields": ["PX_LAST"], "data": [{"date": TS(2018, 1, 3), "PX_LAST": 126.09}]},
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_reference():
    request = {"ReferenceDataRequest": {"fields": ["NAME"], "securities": ["SPY US Equity", "TLT US Equity"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [10],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "SPY US Equity",
                                "eidData": [],
                                "fieldExceptions": [],
                                "sequenceNumber": 0,
                                "fieldData": {"fieldData": {"NAME": "SPDR S&P 500 ETF TRUST"}},
                            }
                        },
                        {
                            "securityData": {
                                "security": "TLT US Equity",
                                "eidData": [],
                                "fieldExceptions": [],
                                "sequenceNumber": 1,
                                "fieldData": {"fieldData": {"NAME": "ISHARES 20+ YEAR TREASURY BO"}},
                            }
                        },
                    ]
                },
            },
        }
    ]
    parsed = [
        {"security": "SPY US Equity", "fields": ["NAME"], "data": {"NAME": "SPDR S&P 500 ETF TRUST"}},
        {"security": "TLT US Equity", "fields": ["NAME"], "data": {"NAME": "ISHARES 20+ YEAR TREASURY BO"}},
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_bulk_reference():
    request = {
        "ReferenceDataRequest": {
            "fields": ["FUT_CHAIN"],
            "securities": ["C 1 Comdty"],
            "overrides": [{"overrides": {"fieldId": "CHAIN_DATE", "value": "20100101"}}],
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [5],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "C 1 Comdty",
                                "eidData": [],
                                "fieldExceptions": [],
                                "sequenceNumber": 0,
                                "fieldData": {
                                    "fieldData": {
                                        "FUT_CHAIN": [
                                            {"FUT_CHAIN": {"Security Description": "C H10 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C K10 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C N10 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C U10 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C Z10 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C H11 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C K11 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C N11 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C U11 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C Z11 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C H12 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C K12 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C N12 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C U12 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C Z12 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C N13 Comdty"}},
                                            {"FUT_CHAIN": {"Security Description": "C Z13 Comdty"}},
                                        ]
                                    }
                                },
                            }
                        }
                    ]
                },
            },
        }
    ]
    parsed = [
        {
            "security": "C 1 Comdty",
            "fields": ["FUT_CHAIN"],
            "data": {
                "FUT_CHAIN": [
                    {"Security Description": "C H10 Comdty"},
                    {"Security Description": "C K10 Comdty"},
                    {"Security Description": "C N10 Comdty"},
                    {"Security Description": "C U10 Comdty"},
                    {"Security Description": "C Z10 Comdty"},
                    {"Security Description": "C H11 Comdty"},
                    {"Security Description": "C K11 Comdty"},
                    {"Security Description": "C N11 Comdty"},
                    {"Security Description": "C U11 Comdty"},
                    {"Security Description": "C Z11 Comdty"},
                    {"Security Description": "C H12 Comdty"},
                    {"Security Description": "C K12 Comdty"},
                    {"Security Description": "C N12 Comdty"},
                    {"Security Description": "C U12 Comdty"},
                    {"Security Description": "C Z12 Comdty"},
                    {"Security Description": "C N13 Comdty"},
                    {"Security Description": "C Z13 Comdty"},
                ]
            },
        }
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_bad_field_reference_unprocessed():
    request = {"ReferenceDataRequest": {"fields": ["not_a_field"], "securities": ["RSF82 Comdty"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [5],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "RSF82 Comdty",
                                "eidData": [],
                                "fieldExceptions": [
                                    {
                                        "fieldExceptions": {
                                            "fieldId": "not_a_field",
                                            "errorInfo": {
                                                "errorInfo": {
                                                    "source": "180::bbdbl9",
                                                    "code": 9,
                                                    "category": "BAD_FLD",
                                                    "message": "Field not valid",
                                                    "subcategory": "INVALID_FIELD",
                                                }
                                            },
                                        }
                                    }
                                ],
                                "sequenceNumber": 0,
                                "fieldData": {"fieldData": {}},
                            }
                        }
                    ]
                },
            },
        }
    ]
    parsed = [{"security": "RSF82 Comdty", "fields": ["not_a_field"], "data": {}}]
    parse_steps = []
    return parse_steps, request, response, parsed


def parser_parse_empty_historical():
    request = {
        "HistoricalDataRequest": {
            "fields": ["PX_VOLUME"],
            "securities": ["DOESCRUD Index"],
            "startDate": "20180102",
            "endDate": "20180203",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [10],
                "messageType": "HistoricalDataResponse",
                "timeReceived": None,
                "element": {
                    "HistoricalDataResponse": {
                        "securityData": {
                            "security": "DOESCRUD Index",
                            "eidData": [],
                            "sequenceNumber": 0,
                            "fieldExceptions": [],
                            "fieldData": [],
                        }
                    }
                },
            },
        }
    ]
    parsed = [{"security": "DOESCRUD Index", "fields": ["PX_VOLUME"], "data": []}]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_empty_reference():
    request = {"ReferenceDataRequest": {"fields": ["ID_EXCH_SYMBOL"], "securities": ["RSF82 Comdty"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [11],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "RSF82 Comdty",
                                "eidData": [],
                                "fieldExceptions": [
                                    {
                                        "fieldExceptions": {
                                            "fieldId": "ID_EXCH_SYMBOL",
                                            "errorInfo": {
                                                "errorInfo": {
                                                    "source": "3589::bbdbd14",
                                                    "code": 9,
                                                    "category": "BAD_FLD",
                                                    "message": "Field not applicable to security",
                                                    "subcategory": "NOT_APPLICABLE_TO_REF_DATA",
                                                }
                                            },
                                        }
                                    }
                                ],
                                "sequenceNumber": 0,
                                "fieldData": {"fieldData": {}},
                            }
                        }
                    ]
                },
            },
        }
    ]
    parsed = [{"security": "RSF82 Comdty", "fields": ["ID_EXCH_SYMBOL"], "data": {"ID_EXCH_SYMBOL": None}}]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_empty_reference_unprocessed():
    request = {"ReferenceDataRequest": {"fields": ["ID_EXCH_SYMBOL"], "securities": ["RSF82 Comdty"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [11],
                "messageType": "ReferenceDataResponse",
                "timeReceived": None,
                "element": {
                    "ReferenceDataResponse": [
                        {
                            "securityData": {
                                "security": "RSF82 Comdty",
                                "eidData": [],
                                "fieldExceptions": [
                                    {
                                        "fieldExceptions": {
                                            "fieldId": "ID_EXCH_SYMBOL",
                                            "errorInfo": {
                                                "errorInfo": {
                                                    "source": "3589::bbdbd14",
                                                    "code": 9,
                                                    "category": "BAD_FLD",
                                                    "message": "Field not applicable to security",
                                                    "subcategory": "NOT_APPLICABLE_TO_REF_DATA",
                                                }
                                            },
                                        }
                                    }
                                ],
                                "sequenceNumber": 0,
                                "fieldData": {"fieldData": {}},
                            }
                        }
                    ]
                },
            },
        }
    ]
    parsed = [{"security": "RSF82 Comdty", "fields": ["ID_EXCH_SYMBOL"], "data": {}}]
    parse_steps = []
    return parse_steps, request, response, parsed


def parser_parse_field_info_multi():
    request = {"FieldInfoRequest": {"id": ["NAME", "VOLUME"]}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [20],
                "messageType": "fieldResponse",
                "timeReceived": None,
                "element": {
                    "fieldResponse": [
                        {
                            "fieldData": {
                                "id": "DS002",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "NAME",
                                        "description": "Name",
                                        "datatype": "String",
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [],
                                        "ftype": "Character",
                                    }
                                },
                            }
                        },
                        {
                            "fieldData": {
                                "id": "RQ013",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "VOLUME",
                                        "description": "Volume - Realtime",
                                        "datatype": "Int64",
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [],
                                        "ftype": "Real",
                                    }
                                },
                            }
                        },
                    ]
                },
            },
        }
    ]
    parsed = [
        {
            "id": ["NAME", "VOLUME"],
            "data": {
                "DS002": {
                    "mnemonic": "NAME",
                    "description": "Name",
                    "datatype": "String",
                    "categoryName": [],
                    "property": [],
                    "overrides": [],
                    "ftype": "Character",
                },
                "RQ013": {
                    "mnemonic": "VOLUME",
                    "description": "Volume - Realtime",
                    "datatype": "Int64",
                    "categoryName": [],
                    "property": [],
                    "overrides": [],
                    "ftype": "Real",
                },
            },
        }
    ]
    parse_steps = []
    return parse_steps, request, response, parsed


def parser_parse_field_list():
    request = {"FieldListRequest": {"fieldType": "All"}}
    response = [
        {
            "eventType": 6,
            "eventTypeName": "blpapi.Event.PARTIAL_RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [20],
                "messageType": "fieldResponse",
                "timeReceived": None,
                "element": {
                    "fieldResponse": [
                        {
                            "fieldData": {
                                "id": "DS002",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "NAME",
                                        "description": "Name",
                                        "datatype": "String",
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [],
                                        "ftype": "Character",
                                    }
                                },
                            }
                        },
                        {
                            "fieldData": {
                                "id": "RQ013",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "VOLUME",
                                        "description": "Volume - Realtime",
                                        "datatype": "Int64",
                                        "categoryName": [],
                                        "property": [],
                                        "overrides": [],
                                        "ftype": "Real",
                                    }
                                },
                            }
                        },
                    ]
                },
            },
        },
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [20],
                "messageType": "fieldResponse",
                "timeReceived": None,
                "element": {
                    "fieldResponse": [
                        {
                            "fieldData": {
                                "id": "A0001",
                                "fieldInfo": {
                                    "fieldInfo": {
                                        "mnemonic": "ARD_REVENUES",
                                        "description": "ARD Revenues",
                                        "datatype": "Double",
                                        "categoryName": [
                                            "Fundamentals/Bloomberg Fundamentals/Standard Labels ARD/Income Statement/Revenues/Income/Gains/Losses on RE/Inv't"  # noqa 501
                                        ],
                                        "property": [],
                                        "overrides": [],
                                        "ftype": "Real",
                                    }
                                },
                            }
                        }
                    ]
                },
            },
        },
    ]
    parsed = [
        {
            "id": ["DS002", "RQ013"],
            "data": {
                "DS002": {
                    "mnemonic": "NAME",
                    "description": "Name",
                    "datatype": "String",
                    "categoryName": [],
                    "property": [],
                    "overrides": [],
                    "ftype": "Character",
                },
                "RQ013": {
                    "mnemonic": "VOLUME",
                    "description": "Volume - Realtime",
                    "datatype": "Int64",
                    "categoryName": [],
                    "property": [],
                    "overrides": [],
                    "ftype": "Real",
                },
            },
        },
        {
            "id": ["A0001"],
            "data": {
                "A0001": {
                    "mnemonic": "ARD_REVENUES",
                    "description": "ARD Revenues",
                    "datatype": "Double",
                    "categoryName": [
                        "Fundamentals/Bloomberg Fundamentals/Standard Labels ARD/Income Statement/Revenues/Income/Gains/Losses on RE/Inv't"  # noqa 501
                    ],
                    "property": [],
                    "overrides": [],
                    "ftype": "Real",
                },
            },
        },
    ]
    parse_steps = []
    return parse_steps, request, response, parsed


def parser_parse_intraday_bar():
    request = {
        "IntradayBarRequest": {
            "eventType": "TRADE",
            "security": "CL1 Comdty",
            "interval": 1,
            "startDateTime": "2019-04-24T08:00:00",
            "endDateTime": "2019-04-24T08:02:00",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [7],
                "messageType": "IntradayBarResponse",
                "timeReceived": None,
                "element": {
                    "IntradayBarResponse": {
                        "barData": {
                            "eidData": [],
                            "delayedSecurity": True,
                            "barTickData": [
                                {
                                    "barTickData": {
                                        "time": TS("2019-04-24 08:00:00"),
                                        "open": 65.85,
                                        "high": 65.89,
                                        "low": 65.85,
                                        "close": 65.86,
                                        "volume": 565,
                                        "numEvents": 209,
                                        "value": 37215.16,
                                    }
                                },
                                {
                                    "barTickData": {
                                        "time": TS("2019-04-24 08:01:00"),
                                        "open": 65.87,
                                        "high": 65.87,
                                        "low": 65.83,
                                        "close": 65.86,
                                        "volume": 382,
                                        "numEvents": 117,
                                        "value": 25154.7,
                                    }
                                },
                            ],
                        }
                    }
                },
            },
        }
    ]
    parsed = [
        {
            "security": "CL1 Comdty",
            "data": [
                {
                    "time": TS("2019-04-24 08:00:00"),
                    "open": 65.85,
                    "high": 65.89,
                    "low": 65.85,
                    "close": 65.86,
                    "volume": 565,
                    "numEvents": 209,
                    "value": 37215.16,
                },
                {
                    "time": TS("2019-04-24 08:01:00"),
                    "open": 65.87,
                    "high": 65.87,
                    "low": 65.83,
                    "close": 65.86,
                    "volume": 382,
                    "numEvents": 117,
                    "value": 25154.7,
                },
            ],
            "events": ["TRADE"],
        }
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_intraday_tick():
    request = {
        "IntradayTickRequest": {
            "eventTypes": ["TRADE"],
            "security": "CL1 Comdty",
            "startDateTime": "2019-04-24T08:00:00",
            "endDateTime": "2019-04-24T08:00:00",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [23],
                "messageType": "IntradayTickResponse",
                "timeReceived": None,
                "element": {
                    "IntradayTickResponse": {
                        "tickData": {
                            "eidData": [],
                            "tickData": [
                                {
                                    "tickData": {
                                        "time": TS("2019-04-24 08:00:00"),
                                        "type": "TRADE",
                                        "value": 65.85,
                                        "size": 4,
                                    }
                                },  # noqa: E501
                                {
                                    "tickData": {
                                        "time": TS("2019-04-24 08:00:00"),
                                        "type": "TRADE",
                                        "value": 65.85,
                                        "size": 2,
                                    }
                                },  # noqa: E501
                            ],
                        }
                    }
                },
            },
        }
    ]
    parsed = [
        {
            "security": "CL1 Comdty",
            "data": [
                {"time": TS("2019-04-24 08:00:00"), "type": "TRADE", "value": 65.85, "size": 4},
                {"time": TS("2019-04-24 08:00:00"), "type": "TRADE", "value": 65.85, "size": 2},
            ],
            "events": ["TRADE"],
        }
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_intraday_tick_multi():
    request = {
        "IntradayTickRequest": {
            "eventTypes": ["BID", "ASK"],
            "security": "CL1 Comdty",
            "startDateTime": "2019-04-24T08:00:00",
            "endDateTime": "2019-04-24T08:00:01",
        }
    }
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [23],
                "messageType": "IntradayTickResponse",
                "timeReceived": None,
                "element": {
                    "IntradayTickResponse": {
                        "tickData": {
                            "eidData": [],
                            "tickData": [
                                {
                                    "tickData": {
                                        "time": TS("2019-04-24 08:00:00"),
                                        "type": "BID",
                                        "value": 65.85,
                                        "size": 4,
                                    }
                                },
                                {
                                    "tickData": {
                                        "time": TS("2019-04-24 08:00:00"),
                                        "type": "BID",
                                        "value": 65.85,
                                        "size": 9,
                                    }
                                },
                                {
                                    "tickData": {
                                        "time": TS("2019-04-24 08:00:00"),
                                        "type": "ASK",
                                        "value": 65.86,
                                        "size": 50,
                                    }
                                },
                            ],
                        }
                    }
                },
            },
        }
    ]
    parsed = [
        {
            "security": "CL1 Comdty",
            "data": [
                {"time": TS("2019-04-24 08:00:00"), "type": "BID", "value": 65.85, "size": 4},
                {"time": TS("2019-04-24 08:00:00"), "type": "BID", "value": 65.85, "size": 9},
                {"time": TS("2019-04-24 08:00:00"), "type": "ASK", "value": 65.86, "size": 50},
            ],
            "events": ["BID", "ASK"],
        }
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_parse_instrument_list():
    request = {"instrumentListRequest": {"maxResults": 3}}
    response = [
        {
            "eventType": 5,
            "eventTypeName": "blpapi.Event.RESPONSE",
            "messageNumber": 0,
            "message": {
                "fragmentType": 0,
                "correlationIds": [7],
                "messageType": "InstrumentListResponse",
                "timeReceived": None,
                "element": {
                    "InstrumentListResponse": {
                        "results": [
                            {"results": {"security": "SPX<index>", "description": "S&P 500 Index"}},
                            {
                                "results": {
                                    "security": "USGG10YR<index>",
                                    "description": "US Generic Govt 10 Year Yield",
                                }
                            },
                            {"results": {"security": "XAU<crncy>", "description": "Gold United States Dollar Spot"}},
                        ]
                    }
                },
            },
        }
    ]
    parsed = [
        {"security": "SPX<index>", "description": "S&P 500 Index"},
        {"security": "USGG10YR<index>", "description": "US Generic Govt 10 Year Yield"},
        {"security": "XAU<crncy>", "description": "Gold United States Dollar Spot"},
    ]
    parse_steps = None
    return parse_steps, request, response, parsed


def parser_data_params(mock=True):
    funcs = [
        parser_parse_historical,
        parser_parse_reference,
        parser_parse_bulk_reference,
        parser_parse_bad_field_reference_unprocessed,
        parser_parse_empty_historical,
        parser_parse_empty_reference,
        parser_parse_empty_reference_unprocessed,
        parser_parse_field_info_multi,
    ]
    # only test mocked versions of these since live data changes
    if mock:
        prefix = "mock_"
        funcs.extend(
            [
                parser_parse_multi_historical,
                parser_parse_intraday_bar,
                parser_parse_intraday_tick,
                parser_parse_intraday_tick_multi,
                parser_parse_field_list,
                parser_parse_instrument_list,
            ]
        )
    else:
        prefix = ""

    ids = []
    argvalues = []
    for f in funcs:
        params = f()
        if not mock:
            params = params[0], params[1], params[3]
        ids.append(prefix + f.__name__)
        argvalues.append(params)
    return {"ids": ids, "argvalues": argvalues}


@pytest.mark.parametrize("parse_steps, bbg_request, bbg_responses, exp_res", **parser_data_params(mock=True))
def test_parser_parse_mock_data_processed(parse_steps, bbg_request, bbg_responses, exp_res):
    parser = blp.BlpParser(parse_steps)
    res = list(itertools.chain.from_iterable((parser(r, bbg_request) for r in bbg_responses)))
    assert res == exp_res


@pytest.mark.parametrize("parse_steps, bbg_request, exp_res", **parser_data_params(mock=False))
def test_parser_parse_real_data_processed(bquery, parse_steps, bbg_request, exp_res):
    parser = blp.BlpParser(parse_steps)
    res = bquery.query(bbg_request, parser, collector=list)
    assert res == exp_res


def test_parser_parse_intraday_bar(bquery):
    sd, ed = get_intraday_dates()
    bar_request = {
        "IntradayBarRequest": {
            "eventType": "TRADE",
            "security": "CL1 Comdty",
            "interval": 1,
            "startDateTime": sd,
            "endDateTime": ed,
        }
    }
    parser = blp.BlpParser()
    res = bquery.query(bar_request, parser, collector=list)
    assert_bar_parsed(res, "CL1 Comdty", "TRADE")


def test_parser_parse_intraday_tick(bquery):
    sd, ed = get_intraday_dates()
    tick_request = {
        "IntradayTickRequest": {
            "eventTypes": ["TRADE"],
            "security": "CL1 Comdty",
            "startDateTime": sd,
            "endDateTime": ed,
        }
    }
    parser = blp.BlpParser()
    res = bquery.query(tick_request, parser, collector=list)
    assert_tick_parsed(res, "CL1 Comdty", "TRADE")


def collector_params():
    bulk_reference_parsed_data = [
        {
            "security": "C 1 Comdty",
            "fields": ["FUT_CHAIN"],
            "data": {"FUT_CHAIN": [{"Security Description": "C H10 Comdty"}]},
        },
        {
            "security": "S 1 Comdty",
            "fields": ["FUT_CHAIN"],
            "data": {"FUT_CHAIN": [{"Security Description": "S F10 Comdty"}, {"Security Description": "S H10 Comdty"}]},
        },
    ]
    bulk_reference_collected_exp = {
        "C 1 Comdty": {"FUT_CHAIN": pandas.DataFrame(["C H10 Comdty"], columns=["Security Description"])},
        "S 1 Comdty": {
            "FUT_CHAIN": pandas.DataFrame(["S F10 Comdty", "S H10 Comdty"], columns=["Security Description"])
        },
    }
    bulk_reference_parsed_data_multi = [
        {
            "security": "BCOM Index",
            "fields": ["INDX_MWEIGHT_HIST"],
            "data": {
                "INDX_MWEIGHT_HIST": [
                    {"Index Member": "BON9", "Percent Weight": 2.89},
                    {"Index Member": "C N9", "Percent Weight": 5.32},
                ]
            },
        }
    ]
    bulk_reference_collected_multi_exp = {
        "BCOM Index": {
            "INDX_MWEIGHT_HIST": pandas.DataFrame({"Index Member": ["BON9", "C N9"], "Percent Weight": [2.89, 5.32]})
        }
    }
    return {
        "ids": ["collect_many_to_bds_single_bulk", "collect_many_to_bds_with_multi_bulk"],
        "argvalues": [
            (bulk_reference_parsed_data, blp.BlpQuery().collect_many_to_bds, bulk_reference_collected_exp,),
            (bulk_reference_parsed_data_multi, blp.BlpQuery().collect_many_to_bds, bulk_reference_collected_multi_exp,),
        ],
    }


@pytest.mark.parametrize("parsed_data, collector, exp_res", **collector_params())
def test_collectors(parsed_data, collector, exp_res):
    res = collector(parsed_data)
    assert res.keys() == exp_res.keys()
    for key in res:
        assert res[key].keys() == exp_res[key].keys()
        for sub_key in res[key]:
            assert_frame_equal(res[key][sub_key], exp_res[key][sub_key])


def test_bdh(bquery):
    df = bquery.bdh(["SPY US Equity"], ["PX_VOLUME"], start_date="20180102", end_date="20180103")
    df_expect = pandas.DataFrame(
        [(TS(2018, 1, 2), "SPY US Equity", 86655749.0), (TS(2018, 1, 3), "SPY US Equity", 90070416.0)],
        columns=["date", "security", "PX_VOLUME"],
    )
    assert_frame_equal(df, df_expect)


@pytest.mark.bbg
def test_bdh_infer():
    host = HOST
    port = PORT
    bquery = blp.BlpQuery(host, port, timeout=QUERY_TIMEOUT, field_column_map={}).start()
    df = bquery.bdh(["SPY US Equity"], ["PX_LAST_ACTUAL"], start_date="20180102", end_date="20180103",)
    df_expect = pandas.DataFrame(
        [(TS(2018, 1, 2), "SPY US Equity", 268.77), (TS(2018, 1, 3), "SPY US Equity", 270.47)],
        columns=["date", "security", "PX_LAST_ACTUAL"],
    )
    assert_frame_equal(df, df_expect)


@pytest.mark.bbg
def test_bdh_coerce_none():
    host = HOST
    port = PORT
    bquery = blp.BlpQuery(
        host, port, timeout=QUERY_TIMEOUT, field_column_map={"PX_VOLUME": lambda x: pandas.Series(x, dtype="float64")}
    ).start()
    df = bquery.bdh(["DOESCRUD Index"], ["PX_VOLUME", "PX_LAST"], start_date="20180105", end_date="20180105",)
    dtypes = {"PX_VOLUME": numpy.dtype("float64")}
    df_expect = pandas.DataFrame(
        [(TS(2018, 1, 5), "DOESCRUD Index", None, 419515.0)], columns=["date", "security", "PX_VOLUME", "PX_LAST"],
    ).astype(dtypes)
    assert_frame_equal(df, df_expect)


def test_bdh_empty(bquery):
    df = bquery.bdh(["DOESCRUD Index"], ["PX_VOLUME"], start_date="20180105", end_date="20180105")
    df_expect = pandas.DataFrame({"date": [], "security": [], "PX_VOLUME": []}).astype("O")
    assert_frame_equal(df, df_expect)


def test_bdp(bquery):
    df = bquery.bdp(["SPY US Equity"], ["ID_EXCH_SYMBOL"])
    df_expect = pandas.DataFrame([("SPY US Equity", "SPY")], columns=["security", "ID_EXCH_SYMBOL"])
    assert_frame_equal(df, df_expect)


@pytest.mark.bbg
def test_bdp_infer():
    host = HOST
    port = PORT
    bquery = blp.BlpQuery(host, port, timeout=QUERY_TIMEOUT).start()
    df = bquery.bdp(["SPY US Equity"], ["NAME"])
    df_expect = pandas.DataFrame([("SPY US Equity", "SPDR S&P 500 ETF TRUST")], columns=["security", "NAME"])
    assert_frame_equal(df, df_expect)


def test_bdp_not_applicable(bquery):
    # test allowing NOT_APPLICABLE_TO_REF_DATA
    df = bquery.bdp(["RSF82 Comdty", "PLG18 Comdty"], ["ID_EXCH_SYMBOL"])
    dtypes = {"security": numpy.dtype("O"), "ID_EXCH_SYMBOL": numpy.dtype("O")}
    df_expect = pandas.DataFrame(
        [["RSF82 Comdty", None], ["PLG18 Comdty", "PL"]], columns=["security", "ID_EXCH_SYMBOL"],
    ).astype(dtypes)
    assert_frame_equal(df, df_expect)


def test_bdp_bulk(bquery):
    # bulk reference error
    with pytest.raises(TypeError):
        bquery.bdp(["BCOM Index"], ["INDX_MWEIGHT"])


def test_bds_non_bulk(bquery):
    # non bulk data
    with pytest.raises(TypeError):
        bquery.bds("SPY US Equity", "PX_LAST")


def test_bdib(bquery):
    sd, ed = get_intraday_dates()
    df = bquery.bdib("CL1 Comdty", "TRADE", interval=1, start_datetime=sd, end_datetime=ed)
    dtype_expect = pandas.Series(
        [
            numpy.dtype("datetime64[ns]"),
            numpy.dtype("float64"),
            numpy.dtype("float64"),
            numpy.dtype("float64"),
            numpy.dtype("float64"),
            numpy.dtype("int64"),
            numpy.dtype("int64"),
            numpy.dtype("float64"),
        ],
        index=["time", "open", "high", "low", "close", "volume", "numEvents", "value"],
    )
    assert_series_equal(df.dtypes, dtype_expect)


def bdit_params():
    dtype_expect = pandas.Series(
        [numpy.dtype("datetime64[ns]"), numpy.dtype("O"), numpy.dtype("float64"), numpy.dtype("int64")],
        index=["time", "type", "value", "size"],
    )
    dtype_expect_cc = pandas.Series(
        [
            numpy.dtype("datetime64[ns]"),
            numpy.dtype("O"),
            numpy.dtype("float64"),
            numpy.dtype("int64"),
            numpy.dtype("O"),
        ],
        index=["time", "type", "value", "size", "conditionCodes"],
    )

    return {
        "ids": ["bdit", "bdit_with_condition_codes"],
        "argvalues": [
            ("CL1 Comdty", ["TRADE"], None, dtype_expect),
            ("CL1 Comdty", ["TRADE"], [("includeConditionCodes", True)], dtype_expect_cc,),
        ],
    }


@pytest.mark.parametrize("ticker, fields, options, exp_res", **bdit_params())
def test_bdit(bquery, ticker, fields, options, exp_res):
    sd, ed = get_intraday_dates()
    df = bquery.bdit(ticker, fields, start_datetime=sd, end_datetime=ed, options=options)
    assert_series_equal(df.dtypes, exp_res)


def test_create_historical_query():
    exp_res = {
        "HistoricalDataRequest": {
            "securities": ["SPY US Equity"],
            "fields": ["PX_LAST"],
            "startDate": "20190101",
            "endDate": "20190110",
        }
    }
    assert blp.create_historical_query("SPY US Equity", "PX_LAST", "20190101", "20190110") == exp_res
    assert blp.create_historical_query(["SPY US Equity"], ["PX_LAST"], "20190101", "20190110") == exp_res
    exp_res = {
        "HistoricalDataRequest": {
            "securities": ["SPY US Equity"],
            "fields": ["PX_LAST"],
            "startDate": "20190101",
            "endDate": "20190110",
            "periodicitySelection": "DAILY",
        }
    }
    res = blp.create_historical_query(
        ["SPY US Equity"], ["PX_LAST"], "20190101", "20190110", options=[("periodicitySelection", "DAILY")],
    )
    assert res == exp_res


def test_create_reference_query():
    exp_res = {"ReferenceDataRequest": {"securities": ["SPY US Equity"], "fields": ["PX_LAST"]}}
    assert blp.create_reference_query("SPY US Equity", "PX_LAST") == exp_res
    assert blp.create_reference_query(["SPY US Equity"], ["PX_LAST"]) == exp_res
    exp_res = {
        "ReferenceDataRequest": {
            "securities": ["SPY US Equity"],
            "fields": ["PX_LAST"],
            "overrides": [{"overrides": {"fieldId": "TIME_ZONE_OVERRIDE", "value": 39}}],
        }
    }
    assert blp.create_reference_query(["SPY US Equity"], ["PX_LAST"], overrides=[("TIME_ZONE_OVERRIDE", 39)]) == exp_res
