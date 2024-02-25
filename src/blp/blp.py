import datetime
import itertools
import os
import json
import logging
import queue
import threading
from numbers import Number
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Sequence, Union

import blpapi
import pandas
import pytz

BBG_MONTH_MAP = dict(zip("FGHJKMNQUVXZ", range(1, 13)))
MONTH_BBG_MAP = {v: k for k, v in BBG_MONTH_MAP.items()}


_EVENT_DICT = {
    blpapi.Event.ADMIN: "blpapi.Event.ADMIN",
    blpapi.Event.AUTHORIZATION_STATUS: "blpapi.Event.AUTHORIZATION_STATUS",
    blpapi.Event.PARTIAL_RESPONSE: "blpapi.Event.PARTIAL_RESPONSE",
    blpapi.Event.REQUEST: "blpapi.Event.REQUEST",
    blpapi.Event.REQUEST_STATUS: "blpapi.Event.REQUEST_STATUS",
    blpapi.Event.RESOLUTION_STATUS: "blpapi.Event.RESOLUTION_STATUS",
    blpapi.Event.RESPONSE: "blpapi.Event.RESPONSE",
    blpapi.Event.SERVICE_STATUS: "blpapi.Event.SERVICE_STATUS",
    blpapi.Event.SESSION_STATUS: "blpapi.Event.SESSION_STATUS",
    blpapi.Event.SUBSCRIPTION_DATA: "blpapi.Event.SUBSCRIPTION_DATA",
    blpapi.Event.SUBSCRIPTION_STATUS: "blpapi.Event.SUBSCRIPTION_STATUS",
    blpapi.Event.TIMEOUT: "blpapi.Event.TIMEOUT",
    blpapi.Event.TOKEN_STATUS: "blpapi.Event.TOKEN_STATUS",
    blpapi.Event.TOPIC_STATUS: "blpapi.Event.TOPIC_STATUS",
    blpapi.Event.UNKNOWN: "blpapi.Event.UNKNOWN",
}

_AUTHORIZATION_SUCCESS = blpapi.Name("AuthorizationSuccess")
_AUTHORIZATION_FAILURE = blpapi.Name("AuthorizationFailure")
_TOKEN_SUCCESS = blpapi.Name("TokenGenerationSuccess")
_TOKEN_FAILURE = blpapi.Name("TokenGenerationFailure")
_SESSION_TERMINATED = blpapi.Name("SessionTerminated")
_SESSION_DOWN = blpapi.Name("SessionConnectionDown")
_MARKET_DATA_EVENTS = [blpapi.Name("MarketDataEvents"), blpapi.Name("MarketBarStart"), blpapi.Name("MarketBarUpdate")]

logger = logging.getLogger(__name__)


class BlpSession:
    def __init__(self, event_handler: Optional[Callable], host: str, port: int, app: Optional[str] = None, tls_pk12_fpath: Optional[str] = None, tls_pk7_path: Optional[str] = None, tls_password: Optional[str] = None, **kwargs):
        """Manage a Bloomberg session.

        A BlpSession is used for managing the lifecycle of a connection to a blpapi.Session. This includes managing
        session options, event handlers and authentication.

        Args:
            event_handler: Event handler, if None this session is synchronous. See blpapi.Session.
            host: Host to connect session on
            port: Port to connect session to
            app: The app to use for the session identity, optional
            tls_pk12_fpath: folder path to pk12 file, optional
            tls_pk7_path: folder path to pk7 file, optional
            tls_password: password for the TLS certificate, optional
            **kwargs: Keyword arguments used in blpapi.SessionOptions

        """
        self.event_handler = event_handler
        self.session_options = self.create_session_options(host, port, app=app, tls_pk12_fpath=tls_pk12_fpath, tls_pk7_path=tls_pk7_path, tls_password=tls_password, **kwargs)
        if event_handler:
            self.session = blpapi.Session(options=self.session_options, eventHandler=event_handler)
        else:
            self.session = blpapi.Session(options=self.session_options, eventHandler=None)
        self.identity = None

    def __repr__(self):
        host, port = (
            self.session_options.serverHost(),
            self.session_options.serverPort(),
        )
        return "{} with <address={}:{}><identity={!r}><eventHandler={!r}>".format(
            type(self), host, port, self.identity, self.event_handler
        )

    @staticmethod
    def create_session_options(host: str, port: int, app: Optional[str] = None, tls_pk12_fpath: Optional[str] = None, tls_pk7_path: Optional[str] = None, tls_password: Optional[str] = None, **kwargs) -> blpapi.SessionOptions:
        """Create blpapi.SessionOptions class used in blpapi.Session.

        Args:
            host: Host to connect session on
            port: Port to connection session to
            app: The app to use for the session identity, optional
            tls_pk12_fpath: folder path to pk12 file, optional
            tls_pk7_path: folder path to pk7 file, optional
            tls_password: password for the TLS certificate, optional
            **kwargs: Keyword args passed to the blpapi.SessionOpts, if authentication is needed use
                setAuthenticationOptions

        Returns: A blpapi.SessionOptions

        """
        session_options = blpapi.SessionOptions()
        kwargs["setServerHost"] = host
        kwargs["setServerPort"] = port
        if app:
            kwargs["setSessionIdentityOptions"] = blpapi.AuthOptions.createWithApp(app)
        # logging and subscription logic does not currently support multiple correlationIds
        kwargs["setAllowMultipleCorrelatorsPerMsg"] = False
        kwargs.setdefault("setAutoRestartOnDisconnection", True)
        kwargs.setdefault("setNumStartAttempts", 1)
        kwargs.setdefault("setRecordSubscriptionDataReceiveTimes", True)

        # set tls record if being passed in
        if tls_password and tls_pk7_path and tls_pk12_fpath:
            tls_options = blpapi.TlsOptions.createFromFiles(
                tls_pk12_fpath,
                tls_password,
                tls_pk7_path
            )
            kwargs["setTlsOptions"] = tls_options

        for key in kwargs:
            getattr(session_options, key)(kwargs[key])
        return session_options

    def authenticate(self, timeout: int = 0) -> None:
        """Authenticate the blpapi.Session.

        Args:
            timeout: Milliseconds to wait for service before the blpapi.EventQueue returns a blpapi.Event.TIMEOUT

        """
        token_event_queue = blpapi.EventQueue()
        self.session.generateToken(eventQueue=token_event_queue)

        event = token_event_queue.nextEvent(timeout)
        for n, msg in enumerate(event):
            if msg.messageType() == _TOKEN_SUCCESS:
                logger.info(f"TOKEN_STATUS - Message {n} - {msg}")
                auth_service = self.session.getService("//blp/apiauth")
                auth_request = auth_service.createAuthorizationRequest()
                auth_request.set("token", msg.getElementAsString("token"))
                identity = self.session.createIdentity()
                logger.info(f"Send authorization request\n{auth_request}")
                self.session.sendAuthorizationRequest(auth_request, identity)
            elif msg.messageType() == _TOKEN_FAILURE:
                raise ConnectionError(f"TOKEN_STATUS - Message {n} - {msg}")

        event = token_event_queue.nextEvent(timeout)
        for n, msg in enumerate(event):
            if msg.messageType() == _AUTHORIZATION_FAILURE:
                raise ConnectionError(f"RESPONSE - Message {n} - {msg}")
            elif msg.messageType() == _AUTHORIZATION_SUCCESS:
                logger.info(f"RESPONSE - Message {n} - {msg}")
                self.identity = identity


class BlpStream(BlpSession):
    event_handler: "EventHandler"

    def __init__(self, host: str = "localhost", port: int = 8194, **kwargs):
        """A class to manage an asynchronous Bloomberg streaming session.

        Args:
            host: Host to connect session on
            port: Port to connection session to
            **kwargs: keyword arguments used in blpapi.SessionOptions

        """
        super().__init__(EventHandler(), host, port, **kwargs)

    def __enter__(self):
        if not self.session.start():
            raise ConnectionError(f"Failed to start {self!r}")
        if self.session_options.authenticationOptions():
            if not self.session.openService("//blp/apiauth"):
                raise ConnectionError(f"Failed to start //blp/apiauth for {self!r}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # exiting gracefully, log correspondingly
        self.event_handler.closed.set()
        self.session.stop()
        logger.debug(f"Closed {self!r}")

    def subscribe(self, sub: Dict, timeout: int = 5) -> Dict:
        """Subscribe to instruments.

        Args:
            sub: Dictionary used to build a blpapi.SubscriptionList of subscriptions
            timeout: Seconds before getting result from the queue.Queue returns a queue.Empty exception

        Returns: A dictionary where the key is the blpapi.correlationId value
        and the value is True if successful, False if there was an error and
        a list of invalid fields if there are exceptions

        Examples:
            >>> bs = BlpStream()
            >>> with bs: # doctest: +SKIP
            ...     bs.subscribe(
            ...       {
            ...          'USDCAD Curncy': {'fields': ['LAST_PRICE']},
            ...          'EURUSD Curncy': {'fields': ['LAST_PRICE', 'BAD_FIELD']},
            ...          'BAD_TICKER': {'fields': ['LAST_PRICE']}
            ...       }
            ...     )
             {'USDCAD Curncy': True, 'EURUSD Curncy': ['BAD_FIELD'], 'BAD_TICKER': False}

        """

        logger.info(f"Subscribing to: {sub}")
        sublist = self.dict_to_sub(sub)
        self.session.subscribe(sublist, identity=self.identity)
        return dict(self.event_handler.subscription_queue.get(timeout=timeout) for _ in range(sublist.size()))

    def resubscribe(self, sub: Dict, timeout: int = 5) -> Dict:
        """Resubscribe to all instruments.

        Args:
            sub: Dictionary used to build a blpapi.SubscriptionList of subscriptions
            timeout: Seconds before getting result from the queue.Queue returns a queue.Empty exception

        Returns: A dictionary where the key is the blpapi.correlationId value and the value is True if successful,
        False if there was an error and a list of invalid fields if there are exceptions

        Notes: The documentation indicates that resubscribing to instruments with invalid correlationIds will be ignored
        however this does not seem to work in practice. All current activate subscriptions must be resubscribed to.
        This may be a quirk of DAPI.

        """
        logger.info(f"Resubscribing to: {sub}")
        sublist = self.dict_to_sub(sub)
        self.session.resubscribe(sublist)
        return dict(self.event_handler.subscription_queue.get(timeout=timeout) for _ in range(sublist.size()))

    def unsubscribe(self, sub: Dict, timeout: int = 5) -> Dict:
        """Unsubscribe from all instruments.

        Args:
            sub: Dictionary used to build a blpapi.SubscriptionList of subscriptions
            timeout: Seconds before getting result from the queue.Queue returns a queue.Empty exception

        Notes: The documentation is somewhat ambiguous but the docs suggest you can unsubscribe from only some
        subscriptions however this does not seem to work in practice. This may be a quirk of DAPI.

        """
        logger.info(f"Unsubscribing to: {sub}")
        sublist = self.dict_to_sub(sub)
        self.session.unsubscribe(sublist)
        return dict(self.event_handler.subscription_queue.get(timeout=timeout) for _ in range(sublist.size()))

    def events(self, timeout: Optional[int] = None) -> Generator:
        """Generator of events.

        Args:
            timeout: Seconds before getting result from the queue.Queue returns a queue.Empty exception

        Returns: A generator of events

        Examples:
            >>> bs = BlpStream()
            >>> events = []
            >>> with bs: # doctest: +SKIP
            ...   subs = bs.subscribe({'USDCAD Curncy': {'fields': ['LAST_PRICE']}})
            ...   for ev in bs.events():
            ...       events.append(ev)
            ...       if len(events) > 2: break

        """
        logger.debug("Enter events")
        while True:
            event = self.event_handler.data_queue.get(timeout=timeout)
            yield event

    @staticmethod
    def dict_to_sub(sub: Dict, use_topic: bool = True) -> blpapi.SubscriptionList:
        """Convert dictionary to blpapi.SubscriptionList.

        Args:
            sub: A dictionary containing topics (Bloomberg securities) with nested dictionaries containg optional
              keys 'fields', 'options', 'correlationID'.
            use_topic (boolean): Indicate whether to use the topic as the correlationID if None
              (otherwise will be Bloomberg automatically generated)

        Returns: A blpapi.SubscriptionList

        """
        subscription = blpapi.SubscriptionList()
        for topic in sub:
            fields = sub[topic].get("fields", None)
            options = sub[topic].get("options", None)
            cid = sub[topic].get("correlationID", None)
            if cid is None and use_topic:
                cid = blpapi.CorrelationId(topic)
            subscription.add(topic, fields=fields, options=options, correlationId=cid)
        return subscription


class EventHandler:
    """A default implementation of an eventHandler used in blpapi.Session."""

    def __init__(self):
        self.data_queue = queue.Queue()
        self.subscription_queue = queue.Queue()
        self.closed = threading.Event()

    def __call__(self, event: blpapi.Event, _):
        try:
            if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                self.marketdata_event(event)
            elif event.eventType() == blpapi.Event.SESSION_STATUS:
                self.session_status_event(event)
            elif event.eventType() == blpapi.Event.SUBSCRIPTION_STATUS:
                self.subscription_status_event(event)
            else:
                self.other_event(event)
        except Exception as e:
            logger.exception(e)
            # as per blpapi.Session docs, this will kill the whole process
            raise e

    def session_status_event(self, event):
        for n, msg in enumerate(event):
            if msg.messageType() == _SESSION_TERMINATED and not self.closed.is_set():
                logger.error(f"SESSION_STATUS - Message {n} - {msg}")
            elif msg.messageType() == _SESSION_DOWN:
                logger.warning(f"SESSION_STATUS - Message {n} - {msg}")
            else:
                logger.info(f"SESSION_STATUS - Message {n} - {msg}")

    def subscription_status_event(self, event):
        for n, msg in enumerate(event):
            cid = msg.correlationIds()[0].value()
            subscription_element = element_to_dict(msg.asElement())
            if "SubscriptionFailure" in subscription_element:
                logger.error(f"SUBSCRIPTION_STATUS - cid: {cid!r} - Message {n} - {msg}")
                self.subscription_queue.put((cid, False))
            elif "SubscriptionStarted" in subscription_element:
                exceptions = subscription_element["SubscriptionStarted"]["exceptions"]
                if len(exceptions) > 0:
                    logger.warning(f"SUBSCRIPTION_STATUS - cid: {cid!r} - Message {n} - {msg}")
                    bad_fields = [exception["exceptions"]["fieldId"] for exception in exceptions]
                    self.subscription_queue.put((cid, bad_fields))
                else:
                    logger.debug(f"SUBSCRIPTION_STATUS - cid: {cid!r} - Message {n} - {msg}")
                    self.subscription_queue.put((cid, True))
            elif "SubscriptionTerminated" in subscription_element:
                logger.debug(f"SUBSCRIPTION_STATUS - cid: {cid!r} - Message {n} - {msg}")
                self.subscription_queue.put((cid, True))
            else:
                logger.debug(f"SUBSCRIPTION_STATUS - cid: {cid!r} - Message {n} - {msg}")

    def marketdata_event(self, event):
        event_name = _EVENT_DICT[event.eventType()]
        for n, msg in enumerate(event):
            if msg.messageType() in _MARKET_DATA_EVENTS:
                self.data_queue.put(message_to_dict(msg))
            else:
                self.other_message(event_name, n, msg)

    @classmethod
    def other_event(cls, event):
        event_name = _EVENT_DICT[event.eventType()]
        for n, msg in enumerate(event):
            cls.other_message(event_name, n, msg)

    @staticmethod
    def other_message(event_name, n, msg):
        logger.info(f"{event_name} - Message {n} - {msg}")


def datetime_converter(value: Union[str, datetime.date, datetime.datetime]) -> pandas.Timestamp:
    ts = pandas.Timestamp(value)
    if ts.tz:
        ts = ts.tz_convert(pytz.FixedOffset(ts.tz.getOffsetInMinutes()))
    return ts


def element_to_value(elem: blpapi.Element) -> Union[pandas.Timestamp, str, Number, None]:
    """Convert a blpapi.Element to its value defined in its datatype with some possible coercisions.

    datetime.datetime -> pandas.Timestamp
    datetime.date -> pandas.Timestamp
    blp.name.Name -> str
    null value -> None
    ValueError Exception -> None

    Args:
        elem: Element to convert

    Returns: A value

    """
    if elem.isNull():
        return None
    else:
        try:
            value = elem.getValue()
            if isinstance(value, blpapi.name.Name):
                return str(value)
            if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                return datetime_converter(value)

            return value
        except ValueError:
            return None


def _element_to_dict(elem: Union[str, blpapi.Element]) -> Any:
    if isinstance(elem, str):
        return elem
    dtype = elem.datatype()
    if dtype == blpapi.DataType.CHOICE:
        return {f"{elem.name()}": _element_to_dict(elem.getChoice())}
    elif elem.isArray():
        return [_element_to_dict(v) for v in elem.values()]
    elif dtype == blpapi.DataType.SEQUENCE:
        return {f"{elem.name()}": {f"{e.name()}": _element_to_dict(e) for e in elem.elements()}}
    else:
        return element_to_value(elem)


def element_to_dict(elem: blpapi.Element) -> Dict:
    """Convert a blpapi.Element to an equivalent dictionary representation.

    Args:
        elem: A blpapi.Element

    Returns: A dictionary representation of blpapi.Element

    """
    return _element_to_dict(elem)


def message_to_dict(msg: blpapi.Message) -> Dict:
    """Convert a blpapi.Message to a dictionary representation.

    Args:
        msg: A blpapi.Message

    Returns: A dictionary with relevant message metadata and data

    """
    return {
        "fragmentType": msg.fragmentType() if hasattr(msg, "fragmentType") else None,
        "correlationIds": [cid.value() for cid in msg.correlationIds()],
        "messageType": f"{msg.messageType()}",
        "timeReceived": _get_time_received(msg),
        "element": element_to_dict(msg.asElement()),
    }


def _get_time_received(msg: blpapi.Message) -> Optional[pandas.Timestamp]:
    try:
        return datetime_converter(msg.timeReceived())
    except ValueError:
        return None


def dict_to_req(request: blpapi.Request, request_data: Dict) -> blpapi.Request:
    """Populate request with data from request_data.

    Args:
        request: Request to populate
        request_data: Data used for populating the request

    Returns: A blpapi.Request

    Notes: An example request data dictionary is

      rdata = {'fields': ['SETTLE_DT'], 'securities': ['AUD Curncy'],
               'overrides': [{'overrides': {'fieldId': 'REFERENCE_DATE', 'value': '20180101'}}]}

    """
    for key, value in request_data.items():
        elem = request.getElement(key)
        if elem.datatype() == blpapi.DataType.SEQUENCE:
            for elem_dict in value:
                if elem.isArray():
                    el = elem.appendElement()
                    for k, vv in elem_dict[key].items():
                        el.setElement(k, vv)
                else:
                    elem.setElement(elem_dict, value[elem_dict])
        elif elem.isArray():
            for v in value:
                elem.appendValue(v)
        elif elem.datatype() == blpapi.DataType.CHOICE:
            for k, v in value.items():
                c = elem.getElement(k)
                if c.isArray():
                    for v_i in v:
                        c.appendValue(v_i)
                else:
                    c.setValue(v)
        else:
            elem.setValue(value)
    return request


class BlpQuery(BlpSession):
    _SERVICES = {
        "HistoricalDataRequest": "//blp/refdata",
        "ReferenceDataRequest": "//blp/refdata",
        "IntradayTickRequest": "//blp/refdata",
        "IntradayBarRequest": "//blp/refdata",
        "BeqsRequest": "//blp/refdata",
        "FieldInfoRequest": "//blp/apiflds",
        "FieldListRequest": "//blp/apiflds",
        "instrumentListRequest": "//blp/instruments",
        "GetFills": "//blp/emsx.history",
        "sendQuery": "//blp/bqlsvc",
    }

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        timeout: int = 10000,
        parser: Optional[Callable] = None,
        field_column_map: Optional[Dict[str, Callable]] = None,
        app=None,
        tls_pk12_fpath=None,
        tls_pk7_path=None,
        tls_password=None,
        **kwargs,
    ):
        """A class to manage a synchronous Bloomberg request/response session.

        Args:
            host: Host to connect session on
            port: Port to connection session to
            timeout: Default milliseconds to wait for service before the blpapi.EventQueue returns blpapi.Event.TIMEOUT
            parser: Callable which parses response and request_data
            field_column_map: A map from bloomberg field name to a callable. It is used by the collectors to ensure
              the correct data type. If a field is missing from the map the default pandas type inference is used.
            app: app name to be included in authentication, optional
            tls_pk12_fpath: folder path to pk12 file, optional
            tls_pk7_path: folder path to pk7 file, optional
            tls_password: password for the TLS certificate, optional
            **kwargs: Keyword arguments used in blpapi.SessionOptions

        Examples:
            >>> BlpQuery()
            <class 'blp.blp.BlpQuery'> with <address=localhost:8194><identity=None><eventHandler=None>

        """
        # TODO change so parser and collector are same object
        if parser is None:
            parser = BlpParser()
        if field_column_map is None:
            field_column_map = dict()
        self.parser = parser
        self._field_column_map = field_column_map
        event_handler = None
        self._started = False
        self._services: Dict[str, blpapi.Service] = {}
        self.timeout = timeout
        super().__init__(event_handler, host, port, app, tls_pk12_fpath, tls_pk7_fpath, tls_password, **kwargs)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        """Start the blpapi.Session and open relevant services."""
        if not self._started:
            self._started = self.session.start()
            if not self._started:
                msg = next(iter(self.session.tryNextEvent()))
                logger.info(f"Failed to connect to Bloomberg:\n{msg}")
                raise ConnectionError(f"Failed to start {self!r}")
            logger.debug(f"Started {self!r}")
            logger.debug(f"{next(iter(self.session.tryNextEvent()))}{next(iter(self.session.tryNextEvent()))}")
        for service in set(self._SERVICES.values()):
            if not self.session.openService(service):
                raise ConnectionError(f"Unknown service {service!r}")
            logger.debug(f"Service {service!r} opened")
            logger.debug(f"{next(iter(self.session.tryNextEvent()))}")
            self._services[service] = self.session.getService(service)
        return self

    def stop(self):
        self.session.stop()

    @staticmethod
    def _pass_through(x, _):
        yield x

    def query(
        self,
        request_data: Dict,
        parse: Optional[Callable] = None,
        collector: Optional[Callable] = None,
        timeout: Optional[int] = None,
    ):
        """Request and parse Bloomberg data.

        Args:
            request_data: A dictionary representing a blpapi.Request, specifying both the service and the data
            parse: Callable which takes a dictionary response and request and yields 0 or more values. If None, use
              default parser. If False, do not parse the response
            collector: Callable which takes an iterable
            timeout: Milliseconds to wait for service before the blpapi.EventQueue returns a blpapi.Event.TIMEOUT

        Returns: A result from collector, if collector=None default is a itertools.chain

        Examples:
            >>> bq = BlpQuery().start() # doctest: +SKIP

            A historical data request collected into a list

            >>> rd = {
            ...  'HistoricalDataRequest': {
            ...    'securities': ['CL1 Comdty'],
            ...    'fields': ['PX_LAST'],
            ...    'startDate': '20190102',
            ...    'endDate': '20190102'
            ...   }
            ... }
            >>> bq.query(rd, collector=list) # doctest: +SKIP
            [
             {
              'security':'CL1 Comdty',
              'fields':['PX_LAST'],
              'data':[{'date': Timestamp('2019-01-02 00:00:00'), 'PX_LAST':46.54}]
             }
            ]

            A historical data request with no parsing collected into a list

            >>> bq.query(rd, collector=list, parse=False) # doctest: +SKIP
            [
               {
                  'eventType':5,
                  'eventTypeName':'blpapi.Event.RESPONSE',
                  'messageNumber':0,
                  'message':{
                     'fragmentType':0,
                     'correlationIds':[8],
                     'messageType':'HistoricalDataResponse',
                     'topicName':'',
                     'timeReceived':None,
                     'element':{
                        'HistoricalDataResponse':{
                           'securityData':{
                              'security':'CL1 Comdty',
                              'eidData':[],
                              'sequenceNumber':0,
                              'fieldExceptions':[],
                              'fieldData':[{'fieldData':{'date': Timestamp('2019-01-02 00:00:00'),'PX_LAST':46.54}}]
                           }
                        }
                     }
                  }
               }
            ]

        """
        if timeout is None:
            timeout = self.timeout
        if parse is False:
            parse = self._pass_through
        elif parse is None:
            parse = self.parser
        data_queue = blpapi.EventQueue()
        request = self.create_request(request_data)
        self.send_request(request, data_queue)
        res = (parse(data, request_data) for data in self.get_response(data_queue, timeout))
        res = itertools.chain.from_iterable(res)  # type: ignore
        if collector:
            res = collector(res)
        return res

    def create_request(self, request_data: Dict) -> blpapi.Request:
        """Create a blpapi.Request.

        Args:
            request_data: A dictionary representing a blpapi.Request, specifying both the service and the data

        Returns: blpapi.Request

        Examples:
            >>> bq = BlpQuery().start() # doctest: +SKIP
            >>> bq.create_request({
            ...  'HistoricalDataRequest': {
            ...    'securities': ['CL1 Comdty'],
            ...    'fields': ['PX_LAST'],
            ...    'startDate': '20190102',
            ...    'endDate': '20190102'
            ...   }
            ... }) # doctest: +SKIP

        """
        operation = list(request_data.keys())[0]
        service = self._services[self._SERVICES[operation]]
        request = service.createRequest(operation)
        rdata = request_data[operation]
        request = dict_to_req(request, rdata)
        return request

    def send_request(
        self,
        request: blpapi.Request,
        data_queue: blpapi.EventQueue,
        correlation_id: Optional[blpapi.CorrelationId] = None,
    ) -> blpapi.CorrelationId:
        """Send a request who's data will be populated into data_queue.

        Args:
            request: Request to send
            data_queue: Queue which response populates
            correlation_id: Id associated with request/response

        Returns: blpapi.CorrelationId associated with the request

        """
        logger.debug(
            f"Sent {request} with identity={self.identity!r}, correlationId={correlation_id!r}, event_queue={data_queue!r}",  # noqa: E501
        )
        cid = self.session.sendRequest(request, self.identity, correlation_id, data_queue)
        return cid

    def get_response(self, data_queue: blpapi.EventQueue, timeout: Optional[int] = None) -> Generator:
        """Yield dictionary representation of blpapi.Messages from a blpapi.EventQueue.

        Args:
            data_queue: Queue which contains response
            timeout: Milliseconds to wait for service before the blpapi.EventQueue returns a blpapi.Event.TIMEOUT

        Returns: A generator of messages translated into a dictionary representation

        """
        if timeout is None:
            timeout = self.timeout
        while True:
            event = data_queue.nextEvent(timeout=timeout)
            event_type = event.eventType()
            event_type_name = _EVENT_DICT[event_type]
            if event_type == blpapi.Event.TIMEOUT:
                raise ConnectionError(f"Unexpected blpapi.Event.TIMEOUT received by {self!r}")
            for n, msg in enumerate(event):
                logger.debug(f"Message {n} in {event_type_name}:{msg}")
                response = {
                    "eventType": event_type,
                    "eventTypeName": event_type_name,
                    "messageNumber": n,
                    "message": message_to_dict(msg),
                }
                yield response
            if event_type == blpapi.Event.RESPONSE:
                return

    def cast_columns(self, df: pandas.DataFrame, fields: Iterable) -> pandas.DataFrame:
        res = {}
        for field in fields:
            col_data = df.get(field)
            if field in self._field_column_map:
                col = self._field_column_map[field]
                col_data = col(col_data)
            res[field] = col_data
        # handle the case where all values are None
        try:
            return pandas.DataFrame(res)
        except ValueError as e:
            if df.empty:
                return pandas.DataFrame(columns=res.keys())
            else:
                raise e

    def bdh(
        self,
        securities: Sequence[str],
        fields: List[str],
        start_date: str,
        end_date: str,
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pandas.DataFrame:
        """Bloomberg historical data request.

        Args:
            securities: list of strings of securities
            fields: list of strings of fields
            start_date: start date as '%Y%m%d'
            end_date: end date as '%Y%m%d'
            overrides: List of tuples containing the field to override and its value
            options: key value pairs to to set in request

        Returns: A pandas.DataFrame with columns ['date', 'security', fields[0], ...]

        """
        query = create_historical_query(securities, fields, start_date, end_date, overrides, options)
        res = self.query(query, self.parser, self.collect_to_bdh)
        dfs = []
        for sec in res:
            dfs.append(res[sec].assign(security=sec))
        df = (
            pandas.concat(dfs)
            .sort_values(by="date", axis=0)
            .loc[:, ["date", "security"] + fields]
            .reset_index(drop=True)
        )
        return df

    def collect_to_bdh(self, responses: Iterable) -> Dict[str, pandas.DataFrame]:
        """Collector for bdh()."""
        dfs: Dict = {}
        for response in responses:
            security = response["security"]
            fields = response["fields"] + ["date"]
            # have not seen example where a HistoricalDataResponse for a single security is broken across
            # multiple PARTIAL_RESONSE/RESPONSE but API docs are vague about whether technically possible
            sec_dfs = dfs.get(security, [])
            df = pandas.DataFrame(response["data"])
            df = self.cast_columns(df, fields)
            sec_dfs.append(df)
            dfs[security] = sec_dfs
        for sec in dfs:
            df_list = dfs[sec]
            if len(df_list) > 1:
                dfs[sec] = pandas.concat(df_list).sort_values(by="date", axis=0, ignore_index=True)
            else:
                dfs[sec] = df_list[0]

        return dfs

    def beqs(
        self,
        screen_name: str,
        screen_type: str = "PRIVATE",
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pandas.DataFrame:
        """Bloomberg equity screening request.

        Args:
            screen_name: name of the screen
            screen_type: type of screen, either 'PRIVATE' or 'GLOBAL'
            overrides: List of tuples containing the field to override and its value
            options: key value pairs to to set in request

        Returns: A pandas.DataFrame with columns ['security', eqs_data[0], ...]
        """
        query = create_eqs_query(screen_name, screen_type, overrides, options)
        df = self.query(query, self.parser, self.collect_to_beqs)
        columns = ["security"] + [col for col in df.columns if col != "security"]
        df = df.sort_values("security").reset_index(drop=True).loc[:, columns]  # Ticker = security
        return df

    def collect_to_beqs(self, responses: Iterable) -> pandas.DataFrame:
        """Collector for beqs()."""
        rows = []
        fields = {"security"}
        for response in responses:
            data = response["data"]
            # possible some fields are missing for different securities
            fields = fields.union(set(response["fields"]))
            data["security"] = response["security"]
            rows.append(data)
        df = pandas.DataFrame(rows)
        return self.cast_columns(df, fields)

    def bql(
        self,
        expression: str,
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pandas.DataFrame:
        """Bloomberg query language request.

        Args:
            expression: BQL expression
            overrides: List of tuples containing the field to override and its value
            options: key value pairs to to set in request

        Returns: A list of pandas.DataFrames with columns ["field", "id", "value"], along with additional secondary columns.

        Examples:
            >>> bquery = blp.BlpQuery().start() # doctest: +SKIP
            >>> result = bquery.bql(expression="get(px_last()) for(['AAPL US Equity', 'IBM US Equity'])") # doctest: +SKIP
            >>> print(result[0]) # doctest: +SKIP

            The resulting DataFrame will look like this:
                field        id              value         DATE                    CURRENCY
            0   px_last()    AAPL US Equity   193.050003   2023-12-26T00:00:00Z    USD
            1   px_last()    IBM US Equity    163.210007   2023-12-26T00:00:00Z    USD
        """  # noqa: E501
        query = create_bql_query(expression, overrides, options)

        bql_parser = BlpParser(
            processor_steps=[
                BlpParser._clean_bql_response,
                BlpParser._validate_event,
                BlpParser._validate_response_error,
            ]
        )

        list_of_dfs = self.query(query, bql_parser, self.collect_to_bql)

        return list_of_dfs

    def collect_to_bql(self, responses: Iterable) -> pandas.DataFrame:
        """Collector for bql()."""
        return [pandas.DataFrame(field) for field in responses]

    def bdp(
        self,
        securities: Sequence[str],
        fields: List[str],
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pandas.DataFrame:
        """Bloomberg reference data point request.

        Args:
            securities: list of strings of securities
            fields: list of strings of fields
            overrides: list of tuples containing the field to override and its value
            options: key value pairs to to set in request

        Returns: A pandas.DataFrame where columns are ['security', field[0], ...]

        """
        query = create_reference_query(securities, fields, overrides, options)
        df = self.query(query, self.parser, self.collect_to_bdp)
        df = df.loc[:, ["security"] + fields]
        return df

    def collect_to_bdp(self, responses: Iterable) -> pandas.DataFrame:
        """Collector for bdp()."""
        rows = []
        fields = {"security"}
        for response in responses:
            data = response["data"]
            # possible some fields are missing for different securities
            fields = fields.union(set(response["fields"]))
            for _, value in data.items():
                if isinstance(value, list):
                    raise TypeError(f"Bulk reference data not supported, expected singleton values but received {data}")
            data["security"] = response["security"]
            rows.append(data)
        df = pandas.DataFrame(rows)
        return self.cast_columns(df, fields)

    def bds(
        self, security: str, field: str, overrides: Optional[Sequence] = None, options: Optional[Dict] = None
    ) -> pandas.DataFrame:
        """Bloomberg reference data set request.

        Args:
            security: String representing security
            field: String representing field
            overrides: List of tuples containing the field to override and its value
            options: key value pairs to to set in request

        Returns: A pandas.DataFrame where columns are data element names

        """
        query = create_reference_query(security, field, overrides, options)
        return self.query(query, self.parser, self.collect_to_bds)

    def collect_to_bds(self, responses: Iterable) -> pandas.DataFrame:
        """Collector for bds()."""
        rows = []
        field = None
        for response in responses:
            keys = list(response["data"].keys())
            if len(keys) > 1:
                raise ValueError(f"responses must have only one field, received {keys}")
            if field is not None and field != keys[0]:
                raise ValueError(f"responses contain different fields, {field} and {keys[0]}")
            field = keys[0]
            data = response["data"][field]
            try:
                rows.extend(data)
            except TypeError:
                raise TypeError(f"response data must be bulk reference data, received {response['data']}")
        df = pandas.DataFrame(rows)
        return self.cast_columns(df, df.columns)

    def collect_many_to_bds(self, responses) -> Dict:
        """Collector to nested dictionary of DataFrames.

        Top level keys are securities, next level keys are fields and values are DataFrame in bds() form

        """
        res: Dict = {}
        for response in responses:
            security = response["security"]
            sec_dict = res.get(security, {})
            for field in response["data"]:
                data = response["data"][field]
                if data:
                    rows = sec_dict.get(field, [])
                    rows.extend(data)
                    sec_dict[field] = rows
            res[security] = sec_dict
        for s in res:
            for f in res[s]:
                # what does res[s][f] look like? can it be passed to to_series directly?
                df = pandas.DataFrame(res[s][f])
                res[s][f] = self.cast_columns(df, df.columns)
        return res

    def bdib(
        self,
        security: str,
        event_type: str,
        interval: int,
        start_datetime: str,
        end_datetime: str,
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pandas.DataFrame:
        """Bloomberg intraday bar data request.

        Args:
            security: Security name
            event_type: Event type {TRADE, BID, ASK, BEST_BID, BEST_ASK}
            interval: Length in minutes of bars {1,...1440}
            start_datetime: UTC datetime as '%Y%-m%-dTHH:MM:SS'
            end_datetime: UTC datetime as '%Y-%m-%dTHH:MM:SS'
            overrides: List of tuples containing the field to override and its value
            options: Key value pairs to to set in request

        Returns: A pandas.DataFrame where columns are
        ['time', open', 'high', 'low', 'close', 'volume', 'numEvents', 'value'] time is UTC and value is the dollar
        volume (I believe)

        """
        query = create_intraday_bar_query(
            security,
            event_type,
            interval,
            start_datetime,
            end_datetime,
            overrides,
            options,
        )
        res = self.query(query, self.parser, self.collect_to_bdib)
        dfs = []
        for key in res:
            dfs.append(res[key])
        df = pandas.concat(dfs, ignore_index=True).sort_values(by="time", axis=0)
        return df

    def collect_to_bdib(self, responses: Iterable) -> Dict:
        """Collector for bdib()."""
        dfs: Dict = {}
        for response in responses:
            security = response["security"]
            event = response["events"][0]
            # accounts for situation where response is broken up over a RESPONSE and PARTIAL_RESPONSE.
            # also supports passing multiple IntradayBarResponses
            key = (security, event)
            bar_data = dfs.get(key, [])
            bar_data.extend(response["data"])
            dfs[key] = bar_data
        fields = ["time", "open", "high", "low", "close", "volume", "numEvents", "value"]
        for key in dfs:
            df = pandas.DataFrame(dfs[key])
            df = self.cast_columns(df, fields)
            dfs[key] = df.sort_values(by="time", axis=0, ignore_index=True)
        return dfs

    def bdit(
        self,
        security: str,
        event_types: Sequence[str],
        start_datetime: str,
        end_datetime: str,
        overrides: Optional[Sequence] = None,
        options: Optional[Dict] = None,
    ) -> pandas.DataFrame:
        """Bloomberg tick data request.

        Args:
            security: Security name
            event_types: List of event types
              {TRADE, BID, ASK, BID_BEST, ASK_BEST, BID_YIELD, ASK_YIELD, MID_PRICE, AT_TRADE, BEST_BID}
            start_datetime: UTC datetime as '%Y%-m%-dTHH:MM:SS'
            end_datetime: UTC datetime as '%Y-%m-%dTHH:MM:SS'
            overrides : List of tuples containing the field to override and its value
            options: Key value pairs to to set in request

        Returns: A pandas.DataFrame where columns are ['time', 'type', 'value', 'size'], time is in UTC. Certain options
        may add various columns

        Examples:
            >>> bq = BlpQuery().start() # doctest: +SKIP
            >>> df = bq.bdit('USDCAD Curncy', ['TRADE'], '2019-04-23T08:00:00', '2019-04-23T13:00:00') # doctest: +SKIP

        Notes: Various options exist to add certain fields, such as ("includeConditionCodes", True) and
        ("includeExchangeCodes", True). See Bloomberg Schema for more info.

        """
        query = create_intraday_tick_query(security, event_types, start_datetime, end_datetime, overrides, options)
        res = self.query(query, self.parser, self.collect_to_bdit)
        dfs = []
        for key in res:
            dfs.append(res[key])
        df = pandas.concat(dfs, ignore_index=True).sort_values(by="time", axis=0, ignore_index=True)
        return df

    def collect_to_bdit(self, responses: Iterable) -> Dict:
        """Collector for bdit()."""
        dfs: Dict = {}
        for response in responses:
            security = response["security"]
            events = tuple(response["events"])
            # accounts for situation where response is broken up over a RESPONSE and PARTIAL_RESPONSE.
            # also supports passing multiple IntradayTickResponses
            key = (security, events)
            bar_data = dfs.get(key, [])
            bar_data.extend(response["data"])
            dfs[key] = bar_data
        fields = ["time", "type", "value", "size"]
        for key in dfs:
            df = pandas.DataFrame(dfs[key])
            cols = fields + [c for c in df.columns if c not in fields]
            df = self.cast_columns(df, cols)
            dfs[key] = df.sort_values(by="time", axis=0, ignore_index=True)
        return dfs


class BlpParser:
    """A callable class with a default response parsing implementation.

    The parse method parses the responses from BlpQuery.get_response into a simplified representation the can easily
    be collected using collectors in BlpQuery.

    Args:
        processor_steps: A list of processors which take in a response and request_data and returns a
          validated and possibly modified response. Processors are called sequentially at the start of parse()
        raise_security_errors: If True, raise errors when response contains an INVALID_SECURITY error, otherwise
          log as a warning. This is ignored if ``processor_steps`` is not None.

    """

    def __init__(self, processor_steps: Optional[Sequence] = None, raise_security_errors: bool = True):
        if processor_steps is None and raise_security_errors:
            processor_steps = [
                self._validate_event,
                self._validate_response_type,
                self._validate_response_error,
                self._validate_security_error,
                self._process_field_exception,
            ]
        elif processor_steps is None:
            processor_steps = [
                self._validate_event,
                self._validate_response_type,
                self._validate_response_error,
                self._warn_security_error,
                self._process_field_exception,
            ]
        self._processor_steps = processor_steps

    @staticmethod
    def _clean_bql_response(response, _):
        """
        The purpose of this method is to standardize a BQL (Bloomberg Query Language) response.
        BQL responses differ from standard responses, hence the need for cleanup to make them more consistent.
        """
        aux = json.loads(response["message"]["element"])

        if aux["responseExceptions"]:
            aux["responseError"] = aux["responseExceptions"][0]["message"]
            del aux["responseExceptions"]

        response["message"]["element"] = {"BQLResponse": aux}

        return response

    @staticmethod
    def _validate_event(response, _):
        if response["eventType"] not in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
            raise TypeError(f"Unknown eventType: {response}")
        return response

    @staticmethod
    def _validate_response_type(response, _):
        rtype = list(response["message"]["element"].keys())[0]
        known_responses = (
            "ReferenceDataResponse",
            "HistoricalDataResponse",
            "IntradayBarResponse",
            "BeqsResponse",
            "IntradayTickResponse",
            "fieldResponse",
            "InstrumentListResponse",
            "GetFillsResponse",
        )
        if rtype not in known_responses:
            raise TypeError(f"Unknown {rtype!r}, must be in {known_responses}")
        return response

    @staticmethod
    def _validate_response_error(response, request):
        rtype = list(response["message"]["element"].keys())[0]
        if "responseError" in response["message"]["element"][rtype]:
            raise TypeError(f"Response contains responseError\nresponse: {response}\nrequest: {request}")
        return response

    @staticmethod
    def _process_field_exception(response, _):
        rtype = list(response["message"]["element"].keys())[0]
        response_data = response["message"]["element"][rtype]
        if rtype in (
            "IntradayBarResponse",
            "IntradayTickResponse",
            "BeqsResponse",
            "fieldResponse",
            "InstrumentListResponse",
            "GetFillsResponse",
        ):
            return response
        if rtype == "HistoricalDataResponse":
            response_data = [response_data]
        for sec_data in response_data:
            field_exceptions = sec_data["securityData"]["fieldExceptions"]
            for fe in field_exceptions:
                fe = fe["fieldExceptions"]
                einfo = fe["errorInfo"]["errorInfo"]
                if einfo["category"] == "BAD_FLD" and einfo["subcategory"] == "NOT_APPLICABLE_TO_REF_DATA":
                    field = fe["fieldId"]
                    sec_data["securityData"]["fieldData"]["fieldData"][field] = None
                else:
                    raise TypeError(f"Response for {sec_data['securityData']['security']} contains fieldException {fe}")
        return response

    @staticmethod
    def _validate_fields_exist(response, request_data):
        rtype = list(response["message"]["element"].keys())[0]
        if rtype != "HistoricalDataResponse":
            return response

        fields = set(request_data["HistoricalDataRequest"]["fields"])
        sec_data = response["message"]["element"]["HistoricalDataResponse"]["securityData"]
        if not sec_data["fieldData"] and fields:
            raise TypeError(f"fieldData for {sec_data['security']!r} is missing fields {fields!r}")
        for fd in sec_data["fieldData"]:
            fd = fd["fieldData"]
            diff = fields.difference(fd.keys())
            if diff:
                raise TypeError(f"fieldData for {sec_data['security']!r} is missing fields {diff!r} in {fd!r}")

    @staticmethod
    def _validate_security_error(response, _):
        rtype = list(response["message"]["element"].keys())[0]
        response_data = response["message"]["element"][rtype]
        if rtype in (
            "IntradayBarResponse",
            "IntradayTickResponse",
            "BeqsResponse",
            "fieldResponse",
            "InstrumentListResponse",
            "GetFillsResponse",
        ):
            return response
        if rtype == "HistoricalDataResponse":
            response_data = [response_data]
        for sec_data in response_data:
            data = sec_data["securityData"]
            if "securityError" in data:
                raise TypeError(f"Response for {data['security']!r} contains securityError {data['securityError']}")
        return response

    @staticmethod
    def _warn_security_error(response, _):
        rtype = list(response["message"]["element"].keys())[0]
        response_data = response["message"]["element"][rtype]
        if rtype in (
            "IntradayBarResponse",
            "IntradayTickResponse",
            "BeqsResponse",
            "fieldResponse",
            "InstrumentListResponse",
            "GetFillsResponse",
        ):
            return response
        if rtype == "HistoricalDataResponse":
            response_data = [response_data]
        for sec_data in response_data:
            data = sec_data["securityData"]
            if "securityError" in data:
                logger.warning(f"Response for {data['security']!r} contains securityError {data['securityError']}")
        return response

    def __call__(self, response, request_data):
        """A default parser to parse dictionary representation of response.

        Parses data response to a generator of dictionaries or raises a TypeError if the response type is unknown.
        There is support for ReferenceDataResponse, HistoricalDataResponse, IntradayBarResponse, IntradayTickResponse,
        fieldResponse, InstrumentListResponse and GetFillsResponse. Parsed dictionaries have the following forms:

        .. code-block:: text

            1. ReferenceDataResponse
                Schema: {'security': <str>, 'fields': <list of str>, 'data': <dict of field:value>}
                Examples:
                    {'security': 'SPY US Equity', 'fields': ['NAME'], 'data': {'NAME': 'SPDR S&P 500 ETF TRUST'}}
                    {
                        'security': 'C 1 Comdty',
                        'fields': ['FUT_CHAIN'],
                        'data': {'FUT_CHAIN': [
                            {'Security Description': 'C H10 Comdty'},
                            {'Security Description': 'C K10 Comdty'}
                        ]}
                    }

            2. HistoricalDataResponse
              Schema: {'security': <str>, 'fields': <list of str>, 'data': <list of dict of field:value>}
              Examples:
                  {
                    'security': 'SPY US Equity',
                    'fields': ['PX_LAST'],
                    'data': [
                      {'date': pandas.Timestamp(2018, 1, 2), 'PX_LAST': 268.77},
                      {'date': pandas.Timestamp(2018, 1, 3), 'PX_LAST': 270.47}
                    ]
                  }

            3. IntradayBarResponse
              Schema: {'security': <str>, 'events': [<str>],
                       'data': <list of {'time': <pandas.Timestamp>, 'open': <float>, 'high': <float>, 'low': <float>,
                                         'close': <float>, 'volume': <int>, 'numEvents': <int>, 'value': <float>}}
                      }
              Examples:
                  {
                    'security': 'CL1 Comdty',
                    'data': [{'time': pandas.Timestamp('2019-04-24 08:00:00'), 'open': 65.85, 'high': 65.89,
                              'low': 65.85, 'close': 65.86, 'volume': 565, 'numEvents': 209, 'value': 37215.16}],
                    'events': ['TRADE']
                  }

            4. IntradayTickResponse
              Schema: {'security': <str>, 'events': <list of str>,
                       'data': <list of  {'time': <pandas.Timestamp>, 'type': <str>, 'value': <float>, 'size': <int>}>}
              Examples:
                  {
                     'security': 'CL1 Comdty',
                     'data': [
                       {'time': pandas.Timestamp('2019-04-24 08:00:00'), 'type': 'BID', 'value': 65.85, 'size': 4},
                       {'time': pandas.Timestamp('2019-04-24 08:00:00'), 'type': 'BID', 'value': 65.85, 'size': 41},
                       {'time': pandas.Timestamp('2019-04-24 08:00:00'), 'type': 'ASK', 'value': 65.86, 'size': 50},
                     ],
                     'events': ['BID', 'ASK']
                  }

            5. fieldResponse
              Schema: {'id': <list of str>, data: {<str>: {field: value}}}
              Examples:
                  {
                    'id': ['PX_LAST', 'NAME'],
                    'data': {
                      'DS002': {
                        'mnemonic': 'NAME',
                        'description': 'Name',
                        'datatype': 'String',
                        'categoryName': [],
                        'property': [],
                        'overrides': [],
                        'ftype': 'Character'
                      },
                     'PR005': {
                       'mnemonic': 'PX_LAST',
                       'description': 'Last Price',
                       'datatype': 'Double',
                       'categoryName': [],
                       'property': [],
                       'overrides': ['PX628', 'DY628',...]
                       'ftype': 'Price'
                      }
                    }
                  }

            6. InstrumentListResponse
            Schema: {'security': <str>, 'description': <str>}
            Examples:
                {
                   'security': 'T<govt>,
                   'description': 'United States Treasury Note/Bond (Multiple Matches)'
                }

            7. GetFillsResponse
            Schema: {'Fills': <dict>}
            Examples:
                {
                    'fills': [
                        {'Ticker': 'GCZ9', 'Exchange': 'CMX', 'Type': 'MKT', ...},
                        {'Ticker': 'SIZ9', 'Exchange': 'CMX', 'Type': 'LMT', ...}
                    ]
                }

        Args:
            response (dict): Representation of a blpapi.Message
            request_data (dict): A dictionary representing a blpapi.Request

        Returns: A generator of responses parsed to dictionaries

        """
        for processor in self._processor_steps:
            response = processor(response, request_data)

        rtype = list(response["message"]["element"].keys())[0]
        if rtype == "ReferenceDataResponse":
            sec_data_parser = self._parse_reference_security_data
        elif rtype == "HistoricalDataResponse":
            sec_data_parser = self._parse_historical_security_data
        elif rtype == "IntradayBarResponse":
            sec_data_parser = self._parse_bar_security_data
        elif rtype == "IntradayTickResponse":
            sec_data_parser = self._parse_tick_security_data
        elif rtype == "BeqsResponse":
            sec_data_parser = self._parse_equity_screening_data
        elif rtype == "BQLResponse":
            sec_data_parser = self._parse_bql_data
        elif rtype == "fieldResponse":
            sec_data_parser = self._parse_field_info_data
        elif rtype == "InstrumentListResponse":
            sec_data_parser = self._parse_instrument_info_data
        elif rtype == "GetFillsResponse":
            sec_data_parser = self._parse_fills_data
        else:
            known_responses = (
                "ReferenceDataResponse",
                "HistoricalDataResponse",
                "IntradayBarResponse",
                "IntradayTickResponse",
                "BeqsResponse",
                "BQLResponse",
                "fieldResponse",
                "InstrumentListResponse",
                "GetFillsResponse",
            )
            raise TypeError(f"Unknown {rtype!r}, must be in {known_responses}")

        return sec_data_parser(response, request_data)

    @staticmethod
    def _parse_reference_security_data(response, request_data):
        rtype = list(response["message"]["element"].keys())[0]
        response_data = response["message"]["element"][rtype]
        req_type = list(request_data.keys())[0]
        for sec_data in response_data:
            result = {
                "security": sec_data["securityData"]["security"],
                "fields": request_data[req_type]["fields"],
            }
            field_data = sec_data["securityData"]["fieldData"]["fieldData"]
            data = {}
            for field in field_data.keys():
                # bulk reference data
                if isinstance(field_data[field], list):
                    rows = []
                    for fd in field_data[field]:
                        datum = {}
                        for name, value in fd[field].items():
                            datum[name] = value
                        rows.append(datum)
                    data[field] = rows
                # reference data
                else:
                    data[field] = field_data[field]
            result["data"] = data
            yield result

    @staticmethod
    def _parse_historical_security_data(response, request_data):
        rtype = list(response["message"]["element"].keys())[0]
        response_data = [response["message"]["element"][rtype]]
        req_type = list(request_data.keys())[0]
        for sec_data in response_data:
            result = {
                "security": sec_data["securityData"]["security"],
                "fields": request_data[req_type]["fields"],
            }
            field_data = sec_data["securityData"]["fieldData"]
            data = []
            for fd in field_data:
                data.append(fd["fieldData"])
            result["data"] = data
            yield result

    @staticmethod
    def _parse_bar_security_data(response, request_data):
        rtype = list(response["message"]["element"].keys())[0]
        bar_data = response["message"]["element"][rtype]["barData"]["barTickData"]
        data = []
        for bd in bar_data:
            data.append(bd["barTickData"])
        req_type = list(request_data.keys())[0]
        result = {
            "security": request_data[req_type]["security"],
            "data": data,
            "events": [request_data[req_type]["eventType"]],
        }
        yield result

    @staticmethod
    def _parse_tick_security_data(response, request_data):
        rtype = list(response["message"]["element"].keys())[0]
        bar_data = response["message"]["element"][rtype]["tickData"]["tickData"]
        data = []
        for bd in bar_data:
            data.append(bd["tickData"])
        req_type = list(request_data.keys())[0]
        result = {
            "security": request_data[req_type]["security"],
            "data": data,
            "events": request_data[req_type]["eventTypes"],
        }
        yield result

    @staticmethod
    def _parse_equity_screening_data(response, _):
        rtype = list(response["message"]["element"].keys())[0]
        response_data = response["message"]["element"][rtype]["data"]
        fields = list(response_data["fieldDisplayUnits"]["fieldDisplayUnits"].keys())

        for sec_data in response_data["securityData"]:
            result = {
                "security": sec_data["securityData"]["security"],
                "fields": fields,
                "data": sec_data["securityData"]["fieldData"]["fieldData"],
            }
            yield result

    @staticmethod
    def _parse_bql_data(response, _):
        rtype = list(response["message"]["element"].keys())[0]
        response_data = response["message"]["element"][rtype]["results"]

        for field in response_data.values():
            # ID column may be a security ticker
            field_data = {
                "field": field["name"],
                "id": field["idColumn"]["values"],
                "value": field["valuesColumn"]["values"],
            }

            # Secondary columns may be DATE or CURRENCY, for example
            for secondary_column in field["secondaryColumns"]:
                field_data[secondary_column["name"]] = secondary_column["values"]

            yield field_data

    @staticmethod
    def _parse_field_info_data(response, request_data):
        rtype = "fieldResponse"
        field_data = response["message"]["element"][rtype]
        data = {}
        for fd in field_data:
            datum = fd["fieldData"]["fieldInfo"]["fieldInfo"]
            data[fd["fieldData"]["id"]] = datum
        if "FieldInfoRequest" in request_data:
            ids = request_data["FieldInfoRequest"]["id"]
        else:
            ids = list(data.keys())
        result = {"id": ids, "data": data}
        yield result

    @staticmethod
    def _parse_instrument_info_data(response, _):
        data = response["message"]["element"]["InstrumentListResponse"]["results"]
        for datum in data:
            result = datum["results"]
            yield result

    @staticmethod
    def _parse_fills_data(response, _):
        data = response["message"]["element"]["GetFillsResponse"]["Fills"]
        result = []
        for datum in data:
            result.append(datum["Fills"])
        yield {"Fills": result}


def create_query(request_type: str, values: Dict, overrides: Optional[Sequence] = None) -> Dict:
    """Create a request dictionary used to construct a blpapi.Request.

    Args:
        request_type: Type of request
        values: key value pairs to set in the request
        overrides: List of tuples containing the field to override and its value

    Returns: A dictionary representation of a blpapi.Request

    Examples:

        Reference data request

        >>> create_query(
        ...   'ReferenceDataRequest',
        ...   {'securities': ['CL1 Comdty', 'CO1 Comdty'], 'fields': ['PX_LAST']}
        ... )
        {'ReferenceDataRequest': {'securities': ['CL1 Comdty', 'CO1 Comdty'], 'fields': ['PX_LAST']}}

        Reference data request with overrides

        >>> create_query(
        ...   'ReferenceDataRequest',
        ...   {'securities': ['AUD Curncy'], 'fields': ['SETTLE_DT']},
        ...   [('REFERENCE_DATE', '20180101')]
        ... )  # noqa: E501
        {'ReferenceDataRequest': {'securities': ['AUD Curncy'], 'fields': ['SETTLE_DT'], 'overrides': [{'overrides': {'fieldId': 'REFERENCE_DATE', 'value': '20180101'}}]}}

        Historical data request

        >>> create_query(
        ...   'HistoricalDataRequest',
        ...   {
        ...    'securities': ['CL1 Comdty'],
        ...    'fields': ['PX_LAST', 'VOLUME'],
        ...    'startDate': '20190101',
        ...    'endDate': '20190110'
        ...   }
        ... )  # noqa: E501
        {'HistoricalDataRequest': {'securities': ['CL1 Comdty'], 'fields': ['PX_LAST', 'VOLUME'], 'startDate': '20190101', 'endDate': '20190110'}}

    """
    request_dict: Dict = {request_type: {}}
    for key in values:
        request_dict[request_type][key] = values[key]
    ovrds = []
    if overrides:
        for field, value in overrides:
            ovrds.append({"overrides": {"fieldId": field, "value": value}})
        request_dict[request_type]["overrides"] = ovrds
    return request_dict


def create_bql_query(
    expression: str,
    overrides: Optional[Sequence] = None,
    options: Optional[Dict] = None,
) -> Dict:
    """Create a sendQuery dictionary request.

    Args:
        expression: BQL query string

    Returns: A dictionary representation of a blpapi.Request
    """
    values = {"expression": expression}
    if options:
        values.update(options)
    return create_query("sendQuery", values, overrides)


def create_eqs_query(
    screen_name: str,
    screen_type: str = "PRIVATE",
    overrides: Optional[Sequence] = None,
    options: Optional[Dict] = None,
) -> Dict:
    """Create a BeqsRequest dictionary request.

    Args:
        screen_name: name of the screen
        screen_type: type of screen; either PRIVATE or GLOBAL
        overrides: List of tuples containing the field to override and its value
        options: key value pairs to to set in request

    Returns: A dictionary representation of a blpapi.Request
    """
    values = {
        "screenName": screen_name,
        "screenType": screen_type,
    }
    if options:
        values.update(options)
    return create_query("BeqsRequest", values, overrides)


def create_historical_query(
    securities: Union[str, Sequence[str]],
    fields: Union[str, Sequence[str]],
    start_date: str,
    end_date: str,
    overrides: Optional[Sequence] = None,
    options: Optional[Dict] = None,
) -> Dict:
    """Create a HistoricalDataRequest dictionary request.

    Args:
        securities: list of strings of securities
        fields: list of strings of fields
        start_date: start date as '%Y%m%d'
        end_date: end date as '%Y%m%d'
        overrides: List of tuples containing the field to override and its value
        options: key value pairs to to set in request

    Returns: A dictionary representation of a blpapi.Request

    """
    if isinstance(securities, str):
        securities = [securities]
    if isinstance(fields, str):
        fields = [fields]
    values = {
        "securities": securities,
        "fields": fields,
        "startDate": start_date,
        "endDate": end_date,
    }
    if options:
        values.update(options)
    return create_query("HistoricalDataRequest", values, overrides)


def create_reference_query(
    securities: Union[str, Sequence[str]],
    fields: Union[str, Sequence[str]],
    overrides: Optional[Sequence] = None,
    options: Optional[Dict] = None,
) -> Dict:
    """Create a ReferenceDataRequest dictionary request.

    Args:
        securities: list of strings of securities
        fields: list of strings of fields
        overrides: List of tuples containing the field to override and its value
        options: key value pairs to to set in request

    Returns: A dictionary representation of a blpapi.Request

    """
    if isinstance(securities, str):
        securities = [securities]
    if isinstance(fields, str):
        fields = [fields]
    values = {"securities": securities, "fields": fields}
    if options:
        values.update(options)
    return create_query("ReferenceDataRequest", values, overrides)


def create_intraday_tick_query(
    security: str,
    event_types: Sequence[str],
    start_datetime: str,
    end_datetime: str,
    overrides: Optional[Sequence] = None,
    options: Optional[Dict] = None,
) -> Dict:
    """Create an IntradayTickRequest.

    Args:
        security: Security name
        event_types: List of event types
          {TRADE, BID, ASK, BID_BEST, ASK_BEST, BID_YIELD, ASK_YIELD, MID_PRICE, AT_TRADE, BEST_BID}
        start_datetime: UTC datetime as '%Y%-m%-dTHH:MM:SS'
        end_datetime: UTC datetime as '%Y-%m-%dTHH:MM:SS'
        overrides: List of tuples containing the field to override and its value
        options: Key value pairs to to set in request

    Returns: A dictionary representation of a blpapi.Request

    """
    if isinstance(event_types, str):
        event_types = [event_types]
    values = {
        "security": security,
        "eventTypes": event_types,
        "startDateTime": start_datetime,
        "endDateTime": end_datetime,
    }
    if options:
        values.update(options)
    return create_query("IntradayTickRequest", values, overrides)


def create_intraday_bar_query(
    security: str,
    event_type: str,
    interval: int,
    start_datetime: str,
    end_datetime: str,
    overrides: Optional[Sequence] = None,
    options: Optional[Dict] = None,
) -> Dict:
    """Create an IntradayBarRequest dictionary request.

    Args:
        security: Security name
        event_type: Event type {TRADE, BID, ASK, BEST_BID, BEST_ASK}
        interval: Length in minutes of bars {1,...1440}
        start_datetime: UTC datetime as '%Y%-m%-dTHH:MM:SS'
        end_datetime: UTC datetime as '%Y-%m-%dTHH:MM:SS'
        overrides: List of tuples containing the field to override and its value
        options: Key value pairs to to set in request

    Returns: A dictionary representation of a blpapi.Request

    """
    values = {
        "security": security,
        "eventType": event_type,
        "startDateTime": start_datetime,
        "endDateTime": end_datetime,
        "interval": interval,
    }
    if options:
        values.update(options)
    return create_query("IntradayBarRequest", values, overrides)


def create_fills_query(start_datetime: str, end_datetime: str, uuids: Sequence[str]) -> Dict:
    """Create a GetFills dictionary request.

    Args:
        start_datetime: UTC datetime as '%Y%-m%-dTHH:MM:SS'
        end_datetime: UTC datetime as '%Y-%m-%dTHH:MM:SS'
        uuids: List of user uuids to get fills associated with

    Returns: A dictionary representation of a blpapi.Request

    """
    return {"GetFills": {"FromDateTime": start_datetime, "ToDateTime": end_datetime, "Scope": {"Uuids": uuids}}}


def create_field_list_query(field_type: Optional[str] = None, field_documentation: bool = True) -> Dict:
    """Create a FieldListRequest dictionary request.

    Args:
        field_type: One of {'All', 'Static', 'RealTime'}
        field_documentation: Return field documentation

    Returns: A dictionary representation of a blpapi.Request

    """
    field_type = field_type or "All"
    return {"FieldListRequest": {"fieldType": field_type, "returnFieldDocumentation": field_documentation}}


def create_instrument_list_query(values: Optional[Dict] = None) -> Dict:
    """Create an instrumentListRequest dictionary request.

    Args:
        values: Values to set in request

    Returns: A dictionary representation of a blpapi.Request

    """
    values = values or {}
    return create_query("instrumentListRequest", values)
