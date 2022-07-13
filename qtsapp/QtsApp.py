import six
import os
import sys
import time
import json
import struct
import logging
import threading
import requests
import re
import pandas as pd
import numpy as np
import xlwings as xw
import datetime as dt
from datetime import datetime as dtdt
from contextlib import closing
from websocket import create_connection, _exceptions
from dotenv import dotenv_values
from datetime import datetime
from twisted.internet import reactor, ssl
from twisted.python import log as twisted_log
from twisted.internet.protocol import ReconnectingClientFactory
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory, connectWS
log = logging.getLogger(__name__)


class QTSAppClientProtocol(WebSocketClientProtocol):
    KEEPALIVE_INTERVAL = 5

    def __init__(self, *args, **kwargs):
        """Initialize protocol with all options passed from factory."""
        super(QTSAppClientProtocol, self).__init__(*args, **kwargs)

    # Overide method
    def onConnect(self, response):  # noqa
        """Called when WebSocket server connection was established"""
        self.factory.ws = self

        if self.factory.on_connect:
            self.factory.on_connect(self, response)

        # Reset reconnect on successful reconnect
        self.factory.resetDelay()

    # Overide method
    def onOpen(self):  # noqa
        """Called when the initial WebSocket opening handshake was completed."""
        if self.factory.on_open:
            self.factory.on_open(self)

    # Overide method
    def onMessage(self, payload, is_binary):  # noqa
        """Called when text or binary message is received."""
        if self.factory.on_message:
            self.factory.on_message(self, payload, is_binary)

    # Overide method
    def onClose(self, was_clean, code, reason):  # noqa
        """Called when connection is closed."""
        if not was_clean:
            if self.factory.on_error:
                self.factory.on_error(self, code, reason)

        if self.factory.on_close:
            self.factory.on_close(self, code, reason)

    def onPong(self, response):  # noqa
        """Called when pong message is received."""
        if self._last_pong_time and self.factory.debug:
            log.debug("last pong was {} seconds back.".format(
                time.time() - self._last_pong_time))

        self._last_pong_time = time.time()

        if self.factory.debug:
            log.debug("pong => {}".format(response))

    # """
    # Custom helper and exposed methods.
    # """
    # drop existing connection to avoid ghost connection
    # self.dropConnection(abort=True)


class QTSAppClientFactory(WebSocketClientFactory, ReconnectingClientFactory):

    """Autobahn WebSocket client factory to implement reconnection and custom callbacks."""

    protocol = QTSAppClientProtocol
    maxDelay = 5
    maxRetries = 10

    _last_connection_time = None

    def __init__(self, *args, **kwargs):
        """Initialize with default callback method values."""
        self.debug = False
        self.ws = None
        self.on_open = None
        self.on_error = None
        self.on_close = None
        self.on_message = None
        self.on_connect = None
        self.on_reconnect = None
        self.on_noreconnect = None

        super(QTSAppClientFactory, self).__init__(*args, **kwargs)

    def startedConnecting(self, connector):  # noqa
        """On connecting start or reconnection."""
        if not self._last_connection_time and self.debug:
            log.debug("Start WebSocket connection.")

        self._last_connection_time = time.time()

    def clientConnectionFailed(self, connector, reason):  # noqa
        """On connection failure (When connect request fails)"""
        if self.retries > 0:
            log.error("Retrying connection. Retry attempt count: {}. Next retry in around: {} seconds".format(
                self.retries, int(round(self.delay))))

            # on reconnect callback
            if self.on_reconnect:
                self.on_reconnect(self.retries)

        # Retry the connection
        self.retry(connector)
        self.send_noreconnect()

    def clientConnectionLost(self, connector, reason):  # noqa
        """On connection lost (When ongoing connection got disconnected)."""
        if self.retries > 0:
            # on reconnect callback
            if self.on_reconnect:
                self.on_reconnect(self.retries)

        # Retry the connection
        self.retry(connector)
        self.send_noreconnect()

    def send_noreconnect(self):
        """Callback `no_reconnect` if max retries are exhausted."""
        if self.maxRetries is not None and (self.retries > self.maxRetries):
            if self.debug:
                log.debug(
                    "Maximum retries ({}) exhausted.".format(self.maxRetries))

            if self.on_noreconnect:
                self.on_noreconnect()


class QTSAppUser(threading.Thread):
    # Default connection timeout
    CONNECT_TIMEOUT = 30
    # Default Reconnect max delay.
    RECONNECT_MAX_DELAY = 60
    # Default reconnect attempts
    RECONNECT_MAX_TRIES = 50

    # Flag to set if its first connect
    _is_first_connect = True

    # Minimum delay which should be set between retries. User can't set less
    # than this
    _minimum_reconnect_max_delay = 5
    # Maximum number or retries user can set
    _maximum_reconnect_max_tries = 300
    ws_endpoints = {"login": "wss://ws.quantsapp.com"}

    def __init__(self,
                 user_name: str=None,
                 password: str=None,
                 api_key: str=None,
                 access_token: str=None,
                 debug=False,
                 reconnect=True,
                 reconnect_max_tries=RECONNECT_MAX_TRIES,
                 reconnect_max_delay=RECONNECT_MAX_DELAY,
                 connect_timeout=CONNECT_TIMEOUT):
        threading.Thread.__init__(self)
        # Set max reconnect tries
        if reconnect_max_tries > self._maximum_reconnect_max_tries:
            log.warning("`reconnect_max_tries` can not be more than {val}. Setting to highest possible value - {val}.".format(
                val=self._maximum_reconnect_max_tries))
            self.reconnect_max_tries = self._maximum_reconnect_max_tries
        else:
            self.reconnect_max_tries = reconnect_max_tries
        # Set max reconnect delay
        if reconnect_max_delay < self._minimum_reconnect_max_delay:
            log.warning("`reconnect_max_delay` can not be less than {val}. Setting to lowest possible value - {val}.".format(
                val=self._minimum_reconnect_max_delay))
            self.reconnect_max_delay = self._minimum_reconnect_max_delay
        else:
            self.reconnect_max_delay = reconnect_max_delay
        self.connect_timeout = connect_timeout
        # Debug enables logs
        self.debug = debug
        # Placeholders for callbacks.
        self.on_ticks = None
        self.on_open = None
        self.on_close = None
        self.on_error = None
        self.on_connect = None
        self.on_message = None
        self.on_reconnect = None
        self.on_noreconnect = None
        # List of current subscribed tokens
        self.subscribed_tokens = []
        self._user_agent = requests.get(
            "https://techfanetechnologies.github.io/latest-user-agent/user_agents.json").json()[-2]
        self._get_app_version()
        self._fetch_master_script()
        self._get_instrument_records()
        self._config = dotenv_values(".env.secret")
        self._user_name = self._config['USER_NAME'] if self._config and len(
            self._config.keys()) != 0 and 'USER_NAME' in self._config.keys() and user_name is None else user_name
        self._password = self._config['PASSWORD'] if self._config and len(
            self._config.keys()) != 0 and 'PASSWORD' in self._config.keys() and password is None else password
        # self._api_key,  self._access_token = api_key, access_token if api_key
        # is not None and access_token is not None else self._auth()
        self._config = dotenv_values(".env")
        self._api_key = self._config['API_KEY'] if self._config and len(
            self._config.keys()) != 0 and 'API_KEY' in self._config.keys() and api_key is None else api_key
        self._access_token = self._config['ACCESS_TOKEN'] if self._config and len(
            self._config.keys()) != 0 and 'ACCESS_TOKEN' in self._config.keys() and access_token is None else access_token
        self._validate_session()
        self.ws_endpoints = self.ws_endpoints | {
            "user": f"wss://wsoc.quantsapp.com/?user_id={self._api_key}&token={self._access_token}&portal=web&version={self._app_version}&country=in",
            "streaming": f"wss://server.quantsapp.com/stream?user_id={self._api_key}&token={self._access_token}&portal=web&version={self._app_version}&country=in&force_login=false"}
        self.socket_url = self.ws_endpoints["user"]
        self._connected_event = threading.Event()

    def _get_app_version(self):
        _BASE_URL = "https://web.quantsapp.com"
        s = requests.get(
            _BASE_URL, headers={'User-Agent': self._user_agent})
        mainjs = re.findall(r'main-es2015\.\w+\.js', s.text)
        mainjs = re.findall(
            r'main.*\w+\.js', s.text)[0].split(" ")[0].replace('"', '')
        mainjs = requests.get(f"{_BASE_URL}/{mainjs}", headers={'User-Agent': self._user_agent})
        kiqv = json.loads(re.findall(
            r'kiQV\:function\(t\)\{t\.exports\=JSON\.parse\(\'..*\'\)\}\,kmnG', mainjs.text)[0].split("\'")[1])
        self._app_name, self._app_version, self._app_key = kiqv[
            "name"], kiqv["version"], kiqv["key"]

    def _auth(self):
        print(self._app_version, self._user_name, self._password,
              self.ws_endpoints["login"].lstrip("wss://"), self._user_agent)
        with closing(create_connection(self.ws_endpoints["login"],
                                       origin="https://web.quantsapp.com",
                                       host="ws.quantsapp.com",
                                       header={'User-Agent': self._user_agent})) as self._ws_auth:
            self._ws_auth.send(
                json.dumps({
                    "noti_token": "0",
                    "action": "signin",
                    "mode": "login_custom",
                    "platform": "web",
                    "version": self._app_version,  # "2.3.57",
                    "country": "in",
                    "email": self._user_name,
                    "user_password": self._password,
                    "sub_platform": "live",
                    "source": "qapp"
                }))
            msg = json.loads(self._ws_auth.recv())
            print(msg)
            if msg["status"] != '1' and msg["msg"] != "Login Successful" and msg["routeKey"] != "signin" and msg["custom_key"] != msg["routeKey"]:
                raise ValueError('failed to authenticate')
        with open(".env", "w") as f:
            f.write(f"API_KEY={msg['api_key']}\n")
            f.write(f"ACCESS_TOKEN={msg['token']}\n")
        return msg["api_key"], msg["token"]

    def _validate_session(self):
        if not _validate_sessions(self._api_key, self._access_token, self._app_version):
            self._api_key, self._access_token = self._auth()

    def _logout(self):
        try:
            self.ws.sendMessage(
                six.b(
                    json.dumps({
                        "mode": "logout",
                        "custom_key": "logout",
                        "action": "user_profile",
                        "country": "in",
                        "version": self._app_version,  # "2.3.57",
                        "platform": "web",
                        "sub_platform": "live"
                    }))
            )
            return True
        except Exception as e:
            self._close(reason="Error while logout: {}".format(str(e)))
            raise
        # {"status": "1", "msg": "Logged out Successfully.", "routeKey": "user_profile", "custom_key": "logout"}

    def _create_connection(self, url, **kwargs):
        """Create a WebSocket client connection."""
        # print(url)
        self.factory = QTSAppClientFactory(url, **kwargs)

        # Alias for current websocket connection
        self.ws = self.factory.ws

        self.factory.debug = self.debug

        # Register private callbacks
        self.factory.on_open = self._on_open
        self.factory.on_error = self._on_error
        self.factory.on_close = self._on_close
        self.factory.on_message = self._on_message
        self.factory.on_connect = self._on_connect
        self.factory.on_reconnect = self._on_reconnect
        self.factory.on_noreconnect = self._on_noreconnect

        self.factory.maxDelay = self.reconnect_max_delay
        self.factory.maxRetries = self.reconnect_max_tries

    def connect(self, threaded=True, disable_ssl_verification=False, proxy=None):
        """
        Establish a websocket connection.

        - `threaded` is a boolean indicating if the websocket client has to be run in threaded mode or not
        - `disable_ssl_verification` disables building ssl context
        - `proxy` is a dictionary with keys `host` and `port` which denotes the proxy settings
        """
        # Custom headers
        headers = {
            'Origin': 'https://web.quantsapp.com',
            'Host': "wsoc.quantsapp.com" if self.socket_url == self.ws_endpoints["user"] else "server.quantsapp.com"
        }

        # Init WebSocket client factory
        self._create_connection(self.socket_url,
                                useragent=self._user_agent,
                                proxy=proxy, headers=headers)

        # Set SSL context
        context_factory = None
        if self.factory.isSecure and not disable_ssl_verification:
            context_factory = ssl.ClientContextFactory()

        # Establish WebSocket connection to a server
        connectWS(self.factory, contextFactory=context_factory,
                  timeout=self.connect_timeout)

        if self.debug:
            twisted_log.startLogging(sys.stdout)

        # Run in seperate thread of blocking
        opts = {}

        # Run when reactor is not running
        if not reactor.running:
            if threaded:
                # Signals are not allowed in non main thread by twisted so
                # suppress it.
                opts["installSignalHandlers"] = False
                self.websocket_thread = threading.Thread(
                    target=reactor.run, kwargs=opts)
                self.websocket_thread.daemon = True
                self.websocket_thread.start()
            else:
                reactor.run(**opts)

    def is_connected(self):
        """Check if WebSocket connection is established."""
        if self.ws and self.ws.state == self.ws.STATE_OPEN:
            return True
        else:
            return False

    def _close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        if self.ws:
            self.ws.sendClose(code, reason)

    def close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        self._logout()
        self.stop_retry()
        self._close(code, reason)

    def stop(self):
        """Stop the event loop. Should be used if main thread has to be closed in `on_close` method.
        Reconnection mechanism cannot happen past this method
        """
        reactor.stop()

    def stop_retry(self):
        """Stop auto retry when it is in progress."""
        if self.factory:
            self.factory.stopTrying()

    def subscribe(self, instrument, expiry):
        """
        Subscribe to a list of instrument_tokens.

        - `instrument_tokens` is list of instrument instrument_tokens to subscribe
        """
        # if isinstance(expiry, str):
        #     expiry = dtdt.strptime(expiry, '%d-%b-%y')
        if isinstance(expiry, dtdt):
            expiry = expiry.strftime("%d-%b-%y")
        self.ws.sendMessage(
            six.b(
                json.dumps({
                    "Scrip": instrument,  # "NIFTY",
                    "Expiry": expiry,  # "07-Jul-22",
                    "custom_key": "chain",
                    "action": "chain-pain-skew-pcr",
                    "platform": "web",
                    "version": self._app_version,  # "2.3.57",
                    "sub_platform": "live"
                }))
        )
        if 0 < len(self.subscribed_tokens) <= 1:
            self.subscribed_tokens[0] = [instrument, expiry]
        else:
            self.subscribed_tokens.append([instrument, expiry])
        return True

    def resubscribe(self):
        """Resubscribe to all current subscribed tokens."""
        instrument, expiry = self.subscribed_tokens[
            0][0], self.subscribed_tokens[0][-1]
        if self.debug:
            log.debug(
                "Resubscribe: {} - {}".format(instrument, expiry))
        self.subscribe(instrument, expiry)

    def _resubscribe_on_instrument_change(self):
        instrument, expiry = self._ws_ir[
            "J3"].value, (self._ws_ir["K3"].value).strftime("%d-%b-%y")
        print(self.subscribed_tokens[0], [instrument, expiry])
        if self.subscribed_tokens[0] != [instrument, expiry]:
            if self.debug:
                log.debug(
                    "Resubscribe: {} - {}".format(instrument, expiry))
            self.subscribe(instrument, expiry)

    def resubscribe_on_instrument_change(self):
        if self._connected_event.wait(timeout=10):
            reactor.callFromThread(self._resubscribe_on_instrument_change)

    def _on_connect(self, ws, response):
        self.ws = ws
        if self.on_connect:
            self.on_connect(self, response)

    def _on_close(self, ws, code, reason):
        """Call `on_close` callback when connection is closed."""
        log.error("Connection closed: {} - {}".format(code, str(reason)))

        if self.on_close:
            self.on_close(self, code, reason)

    def _on_error(self, ws, code, reason):
        """Call `on_error` callback when connection throws an error."""
        log.error("Connection error: {} - {}".format(code, str(reason)))

        if self.on_error:
            self.on_error(self, code, reason)

    def _on_message(self, ws, payload, is_binary):
        """Call `on_message` callback when text message is received."""
        if self.on_message:
            self.on_message(self, payload, is_binary)

        # If the message is text, parse it and send it to the callback.
        # if self.on_ticks and not is_binary and len(payload) > 4:
        #     self.on_ticks(self, self._parse_text_message(payload))

        # Parse text messages
        if not is_binary:
            self._parse_text_message(payload)

    def _on_open(self, ws):
        # self._on_init_get_option_chain()
        # Resubscribe if its reconnect
        if not self._is_first_connect:
            self.resubscribe_on_instrument_change()

        # Set first connect to false once its connected first time
        self._is_first_connect = False

        if self.on_open:
            return self.on_open(self)

    def _on_reconnect(self, attempts_count):
        if self.on_reconnect:
            return self.on_reconnect(self, attempts_count)

    def _on_noreconnect(self):
        if self.on_noreconnect:
            return self.on_noreconnect(self)

    def _parse_text_message(self, payload):
        """Parse text message."""
        # Decode unicode data
        if not six.PY2 and type(payload) == bytes:
            payload = payload.decode("utf-8")

        try:
            data = json.loads(payload)
            if data["status"] != '1':
                raise ValueError(f'Request UnSuccessfull with msg : {data}')
            elif data["custom_key"] == "chain" and data["routeKey"] == "chain-pain-skew-pcr":
                print(data)
                self._populate_oc_table_data(data)
                data = self._df, self._atm_strike, self._maxcalloi, self._maxcalloi_strike, self._maxputoi, self._maxputoi_strike
            elif (data["status"] == '1' and data["msg"] == "Logged out Successfully."
                    and data["routeKey"] == "user_profile" and data["custom_key"] == "logout"):
                print('Logged out Successfully.')
                print(data)
                data = data
            else:
                print(data)
                data = data
        except ValueError:
            return

    def _fetch_master_script(self):
        self._master_script = requests.get(
            "https://techfanetechnologies.github.io/QtSAppMasterScript/masterScript.json").json()  # noqa: E501
        self._instruments = self._master_script.keys()

    def _get_instrument_records(self):
        _instrument_records = [
            ["SymbolName", "ExpiryDate", "LotSize", "Strikes"]]
        for _symbol in self._instruments:
            _instrument_records.extend([[_symbol, self._master_script[_symbol]["expiry"][_idx], lot, len(  # noqa: E501
                self._master_script[_symbol]["strikes"][0])] for _idx, lot in enumerate(self._master_script[_symbol]["lot"])])  # noqa: E501
        self._instrument_records = pd.DataFrame(_instrument_records)

    def _get_lot_value(self, _symbol: str, _expiry: str):
        if _symbol.upper() in self._instruents and _expiry in self._master_script[_symbol]["expiry"]:  # noqa: E501
            return self._master_script[_symbol]["lot"][self._master_script[_symbol]["expiry"].index(_expiry)]  # noqa: E501
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_expiry_dates(self, _symbol: str):
        if _symbol.upper() in self._instruents:
            return self._master_script[_symbol]["expiry"]
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_list_of_strikes(self, _symbol: str, _expiry: str):
        if _symbol.upper() in self._instruents and _expiry in self._master_script[_symbol]["expiry"]:  # noqa: E501
            return self._master_script[_symbol]["strikes"][self._master_script[_symbol]["expiry"].index(_expiry)]  # noqa: E501
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_atm_strike(self, _symbol: str, _expiry: str, _ltp: float):
        all_strikes = self._get_list_of_strikes(_symbol, _expiry)
        atm_strike = int(min(all_strikes, key=lambda x: abs(x - _ltp)))
        return atm_strike

    def _find_atm_strike(self, _all_strikes: pd.core.series.Series, _ltp: float):  # noqa: E501
        return int(min(_all_strikes.astype(int).tolist(), key=lambda x: abs(x - _ltp)))  # noqa: E501

    def _set_df_column_value_at_index(self, _index: int, _column_name: str, _value, _fast=True):  # noqa: E501
        if _fast:
            self._df.at[_index, _column_name] = _value
        else:
            self._df.iloc[
                _index, self._df.columns.get_loc(_column_name)] = _value

    def _get_df_column_value_at_index(self, _index: int, _column_name: str, _fast=True):  # noqa: E501
        if _fast:
            return self._df.at[_index, _column_name]
        else:
            return self._df.iloc[_index, self._df.columns.get_loc(_column_name)]  # noqa: E501

    def _is_fut_or_fair_price(self, price: list):
        future_place = price[-1]
        if future_place == 0:
            return "FuturePrice"
        elif future_place == 1:
            return "FairPrice"
        else:
            return "Unknown"

    def _is_time_between(self, begin_time, end_time, check_time=None):
        from datetime import datetime, time
        # If check time is not given, default to current UTC time
        check_time = check_time or datetime.utcnow().time()
        if begin_time < end_time:
            return check_time >= begin_time and check_time <= end_time
        else:  # crosses midnight
            return check_time >= begin_time or check_time <= end_time

    def _isNowInTimePeriod(self, startTime, endTime, nowTime):
        if startTime < endTime:
            return nowTime >= startTime and nowTime <= endTime
        else:
            # Over midnight:
            return nowTime >= startTime or nowTime <= endTime

    def _populate_oc_table_data(self, _response):
        if _response["status"] == "1" and _response["msg"] == "success" and _response["statusCode"] == 200:  # noqa: E501
            timestamp = _response["timestamp"]
            strike_list = list(map(int, _response["strike"].split(",")))
            strike_list_length_int_array = [int(0)] * len(strike_list)
            strike_list_length_str_array = [str(0)] * len(strike_list)
            strike_list_length_float_array = [float(0)] * len(strike_list)
            oc_table = {
                "PutClose":
                    strike_list_length_float_array,
                "PutLow":
                    strike_list_length_float_array,
                "PutHigh":
                    strike_list_length_float_array,
                "PutOpen":
                    strike_list_length_float_array,
                "PutVetaChange":
                    strike_list_length_float_array,
                "PutVeta":
                    strike_list_length_float_array,
                "PutVolgaChange":
                    strike_list_length_float_array,
                "PutVolga":
                    strike_list_length_float_array,
                "PutColorChange":
                    strike_list_length_float_array,
                "PutColor":
                    strike_list_length_float_array,
                "PutZommaChange":
                    strike_list_length_float_array,
                "PutZomma":
                    strike_list_length_float_array,
                "PutSpeedChange":
                    strike_list_length_float_array,
                "PutSpeed":
                    strike_list_length_float_array,
                "PutCharmChange":
                    strike_list_length_float_array,
                "PutCharm":
                    strike_list_length_float_array,
                "PutVannaChange":
                    strike_list_length_float_array,
                "PutVanna":
                    strike_list_length_float_array,
                "PutGammaChange":
                    strike_list_length_float_array,
                "PutGamma":
                    list(map(float, _response["p_gamma"].split(","))),
                "PutVegaChange":
                    strike_list_length_float_array,
                "PutVega":
                    list(map(float, _response["p_vega"].split(","))),
                "PutThetaChange":
                    strike_list_length_float_array,
                "PutTheta":
                    list(map(float, _response["p_theta"].split(","))),
                "PutDeltaChange":
                    strike_list_length_float_array,
                "PutDelta":
                    list(map(float, _response["p_delta"].split(","))),
                "PutPrevIV":
                    list(map(float, _response["p_prev_iv"].split(","))),
                "PutVolume":
                    list(map(int, _response["p_volume"].split(","))),
                "PutOIChange":
                    list(map(int, _response["p_oi_change"].split(","))),
                "PutOI":
                    list(map(int, _response["p_oi"].split(","))),
                "PutIV":
                    list(map(float, _response["p_iv"].split(","))),
                "PutLtpChange":
                    list(map(float, _response["p_ltp_change"].split(","))),
                "PutLtp":
                    list(map(float, _response["p_ltp"].split(","))),
                "StrikePrice":
                    strike_list,
                "CallLtp":
                    list(map(float, _response["c_ltp"].split(","))),
                "CallLtpChange":
                    list(map(float, _response["c_ltp_change"].split(","))),
                "CallIV":
                    list(map(float, _response["c_iv"].split(","))),
                "CallOI":
                    list(map(int, _response["c_oi"].split(","))),
                "CallOIChange":
                    list(map(int, _response["c_oi_change"].split(","))),
                "CallVolume":
                    list(map(int, _response["c_volume"].split(","))),
                "CallDelta":
                    list(map(float, _response["c_delta"].split(","))),
                "CallDeltaChange":
                    strike_list_length_float_array,
                "CallTheta":
                    list(map(float, _response["c_theta"].split(","))),
                "CallThetaChange":
                    strike_list_length_float_array,
                "CallVega":
                    list(map(float, _response["c_vega"].split(","))),
                "CallVegaChange":
                    strike_list_length_float_array,
                "CallGamma":
                    list(map(float, _response["c_gamma"].split(","))),
                "CallGammaChange":
                    strike_list_length_float_array,
                "CallPrevIV":
                    list(map(float, _response["c_prev_iv"].split(","))),
                "CallVanna":
                    strike_list_length_float_array,
                "CallVannaChange":
                    strike_list_length_float_array,
                "CallCharm":
                    strike_list_length_float_array,
                "CallCharmChange":
                    strike_list_length_float_array,
                "CallSpeed":
                    strike_list_length_float_array,
                "CallSpeedChange":
                    strike_list_length_float_array,
                "CallZomma":
                    strike_list_length_float_array,
                "CallZommaChange":
                    strike_list_length_float_array,
                "CallColor":
                    strike_list_length_float_array,
                "CallColorChange":
                    strike_list_length_float_array,
                "CallVolga":
                    strike_list_length_float_array,
                "CallVolgaChange":
                    strike_list_length_float_array,
                "CallVeta":
                    strike_list_length_float_array,
                "CallVetaChange":
                    strike_list_length_float_array,
                "CallOpen":
                    strike_list_length_float_array,
                "CallHigh":
                    strike_list_length_float_array,
                "CallLow":
                    strike_list_length_float_array,
                "CallClose":
                    strike_list_length_float_array,
                "CallAskPrice":
                    strike_list_length_float_array,
                "CallAskQty":
                    strike_list_length_int_array,
                "CallBidPrice":
                    strike_list_length_float_array,
                "CallBidQty":
                    strike_list_length_int_array,
                "CallAveragePrice":
                    strike_list_length_float_array,
                "PutAskPrice":
                    strike_list_length_float_array,
                "PutAskQty":
                    strike_list_length_int_array,
                "PutBidPrice":
                    strike_list_length_float_array,
                "PutBidQty":
                    strike_list_length_int_array,
                "PutAveragePrice":
                    strike_list_length_float_array,
                "CallDataUpdateTimeStamp":
                    list(map(lambda x: timestamp, strike_list_length_str_array)),  # noqa: E501
                "PutDataUpdateTimeStamp":
                    list(map(lambda x: timestamp, strike_list_length_str_array)),  # noqa: E501
                "FuturePrice":
                    strike_list_length_float_array,
                "FutureOrFairPrice":
                    strike_list_length_str_array,
                "FutureDataUpdateTimeStamp":
                    strike_list_length_str_array,
            }
            futureprice = float(_response["rp"].split(",")[0])
            futureorfairprice = "FuturePrice" if _response["rp"].split(
                ",")[1] == '0' else "FairPrice" if _response["rp"].split(",")[1] == '1' else "Unknown"  # noqa: E501
            futuredataupdatetimestamp = _response["timestamp"]
            self._df = pd.DataFrame.from_dict(oc_table)
            self._atm_strike = _response["astrike"] if self._find_atm_strike(self._df["StrikePrice"], futureprice) == _response[  # noqa: E501
                "astrike"] else self._find_atm_strike(self._df["StrikePrice"], futureprice)  # noqa: E501
            self._df.eval("PutIVChange = PutIV - PutPrevIV", inplace=True)
            self._df.eval(
                "PutIVChangePercent = PutIVChange / PutPrevIV", inplace=True)  # noqa: E501
            self._df.eval("PutPrevOI = PutOI - PutOIChange", inplace=True)
            self._df.eval(
                "PutOIChangePercent = PutOIChange / PutPrevOI", inplace=True)  # noqa: E501
            self._df.eval(
                "PutPrevLTP = PutLtp - PutLtpChange", inplace=True)
            self._df.eval(
                "PutLtpChangePercent = PutLtpChange / PutPrevLTP", inplace=True)  # noqa: E501
            self._df.eval(
                "CallPrevLTP = CallLtp - CallLtpChange", inplace=True)
            self._df.eval(
                "CallLtpChangePercent = CallLtpChange / CallPrevLTP", inplace=True)  # noqa: E501
            self._df.eval(
                "CallPrevOI = CallOI - CallOIChange", inplace=True)
            self._df.eval(
                "CallOIChangePercent = CallOIChange / CallPrevOI", inplace=True)  # noqa: E501
            self._df.eval(
                "CallIVChange = CallIV - CallPrevIV", inplace=True)
            self._df.eval(
                "CallIVChangePercent = CallIVChange / CallPrevIV", inplace=True)  # noqa: E501
            self._df.eval("immediatePrevCLtp = CallPrevLTP", inplace=True)
            self._df.eval("immediatePrevPLtp = PutPrevLTP", inplace=True)
            self._df.eval("immediatePrevFLtp = FuturePrice", inplace=True)
            self._df.insert(97, "CLtpColor", np.select(((self._df["CallPrevLTP"] - self._df["CallLtp"]) > 0, (self._df[  # noqa: E501
                      "CallPrevLTP"] - self._df["CallLtp"]) < 0), ("reself._dfontColor", "greenFontColor"), "whiteFontColor"))  # noqa: E501
            self._df.insert(98, "PLtpColor", np.select(((self._df["PutPrevLTP"] - self._df["PutLtp"]) > 0, (self._df[  # noqa: E501
                      "PutPrevLTP"] - self._df["PutLtp"]) < 0), ("reself._dfontColor", "greenFontColor"), "whiteFontColor"))  # noqa: E501
            self._df.insert(99, "FLtpColor", np.select(((self._df["immediatePrevFLtp"] - self._df["FuturePrice"]) > 0, (self._df[  # noqa: E501
                      "immediatePrevFLtp"] - self._df["FuturePrice"]) < 0), ("reself._dfontColor", "greenFontColor"), "whiteFontColor"))  # noqa: E501
            self._set_df_column_value_at_index(
                0, "FuturePrice", futureprice)
            self._set_df_column_value_at_index(
                0, "FutureOrFairPrice", futureorfairprice)
            self._set_df_column_value_at_index(
                0, "FutureDataUpdateTimeStamp", futuredataupdatetimestamp)
            self._maxcalloi, self._maxcalloi_idx = self._df["CallOI"].astype(  # noqa: E501
                int).max(), self._df["CallOI"].astype(int).idxmax()
            self._maxcalloi_strike = self._df[
                "StrikePrice"].iloc[self._maxcalloi_idx]
            self._maxputoi, self._maxputoi_idx = self._df["PutOI"].astype(  # noqa: E501
                int).max(), self._df["PutOI"].astype(int).idxmax()
            self._maxputoi_strike = self._df[
                "StrikePrice"].iloc[self._maxputoi_idx]
            self._update_option_chain_in_excel_wb()

    def _check_n_return_file_path(self, _file_path):
        try:
            if ((os.path.exists(os.path.join(os.getcwd(), _file_path))
                    and os.path.isfile(os.path.join(os.getcwd(), _file_path)))
                    and not os.path.isdir(os.path.join(os.getcwd(), _file_path))):  # noqa: E501
                _file_path = os.path.join(os.getcwd(), _file_path)
                return _file_path
            else:
                print(f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again")  # noqa: E501
                raise ValueError(f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again")  # noqa: E501
        except ValueError:
            raise ValueError(f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again")  # noqa: E501

    def _update_excel_wb(self, _wb_name: str=None):
        _wb_name = "OptionChain" if not _wb_name else _wb_name
        self._wb = xw.Book(
            self._check_n_return_file_path(f"{_wb_name}.xlsx"))
        self._ws_ir = self._wb.sheets["IR"]
        self._ws_rawdata = self._wb.sheets["RawData"]
        self._ws_ir["A1"].options(pd.DataFrame, index=False, header=False,
                                  expand='table').value = self._instrument_records

    def _update_option_chain_in_excel_wb(self, _streaming=False):
        # while True:
        self._ws_rawdata["C1"].options(
            pd.DataFrame, index=True, header=True, expand='table').value = self._df  # noqa: E501
        if _streaming:
            self._atm_strike = self._find_atm_strike(
                self._df["StrikePrice"], self._get_df_column_value_at_index(0, "FuturePrice"))  # noqa: E501
            self._maxcalloi, self._maxcalloi_idx = self._df["CallOI"].astype(  # noqa: E501
                int).max(), self._df["CallOI"].astype(int).idxmax()
            self._maxcalloi_strike = self._df[
                "StrikePrice"].iloc[self._maxcalloi_idx]
            self._maxputoi, self._maxputoi_idx = self._df["PutOI"].astype(  # noqa: E501
                int).max(), self._df["PutOI"].astype(int).idxmax()
            self._maxputoi_strike = self._df[
                "StrikePrice"].iloc[self._maxputoi_idx]
        self._ws_rawdata["A1"].value, self._ws_rawdata["A2"].value, self._ws_rawdata["A3"].value, self._ws_rawdata["A4"].value, self._ws_rawdata["A5"].value, self._ws_rawdata[  # noqa: E501
            "A6"].value, self._ws_rawdata["A7"].value = "ATMStrike", "MaxCallOI", "MaxCallOIIndex", "MaxCallOIStrike", "MaxPutOI", "MaxPutOIIndex", "MaxPutOIStrike"  # noqa: E501

        self._ws_rawdata["B1"].value, self._ws_rawdata["B2"].value, self._ws_rawdata["B3"].value, self._ws_rawdata["B4"].value, self._ws_rawdata["B5"].value, self._ws_rawdata[  # noqa: E501
            "B6"].value, self._ws_rawdata["B7"].value = self._atm_strike, self._maxcalloi, self._maxcalloi_idx, self._maxcalloi_strike, self._maxputoi, self._maxputoi_idx, self._maxputoi_strike  # noqa: E501

    def _on_init_get_option_chain(self, _symbol: str=None, _expiry: str=None):
        self._update_excel_wb()
        if not _symbol and not _expiry:
            _symbol = self._ws_ir["J3"].value
            _expiry = self._ws_ir["K3"].value
        _instrument = f'{_symbol}:{_expiry.strftime("%d%m%Y")}'
        print(f'{_instrument}')
        self.subscribe(_symbol, _expiry.strftime("%d-%b-%y"))


class QTSAppStream(threading.Thread):
    FORMAT_CHARACTERS = {'?': {'C_Type': '_Bool',
                               'Format': '?',
                               'Python_Type': 'bool',
                               'Standard_Size': 1},
                         'B': {'C_Type': 'unsigned char',
                               'Format': 'B',
                               'Python_Type': 'integer',
                               'Standard_Size': 1},
                         'H': {'C_Type': 'unsigned short',
                               'Format': 'H',
                               'Python_Type': 'integer',
                               'Standard_Size': 2},
                         'I': {'C_Type': 'unsigned int',
                               'Format': 'I',
                               'Python_Type': 'integer',
                               'Standard_Size': 4},
                         'L': {'C_Type': 'unsigned long',
                               'Format': 'L',
                               'Python_Type': 'integer',
                               'Standard_Size': 4},
                         'N': {'C_Type': 'size_t',
                               'Format': 'N',
                               'Python_Type': 'integer',
                               'Standard_Size': 0},
                         'Q': {'C_Type': 'unsigned long long',
                               'Format': 'Q',
                               'Python_Type': 'integer',
                               'Standard_Size': 8},
                         'b': {'C_Type': 'signed char',
                               'Format': 'b',
                               'Python_Type': 'integer',
                               'Standard_Size': 1},
                         'c': {'C_Type': 'char',
                               'Format': 'c',
                               'Python_Type': 'bytes of length 1',
                               'Standard_Size': 1},
                         'e': {'C_Type': '-6',
                               'Format': 'e',
                               'Python_Type': 'float',
                               'Standard_Size': 2},
                         'f': {'C_Type': 'float',
                               'Format': 'f',
                               'Python_Type': 'float',
                               'Standard_Size': 4},
                         'h': {'C_Type': 'short',
                               'Format': 'h',
                               'Python_Type': 'integer',
                               'Standard_Size': 2},
                         'i': {'C_Type': 'int',
                               'Format': 'i',
                               'Python_Type': 'integer',
                               'Standard_Size': 4},
                         'l': {'C_Type': 'long',
                               'Format': 'l',
                               'Python_Type': 'integer',
                               'Standard_Size': 4},
                         'n': {'C_Type': 'ssize_t',
                               'Format': 'n',
                               'Python_Type': 'integer',
                               'Standard_Size': 0},
                         'q': {'C_Type': 'long long',
                               'Format': 'q',
                               'Python_Type': 'integer',
                               'Standard_Size': 8},
                         'x': {'C_Type': 'pad byte',
                               'Format': 'x',
                               'Python_Type': 'no value',
                               'Standard_Size': 0}}

    # Default connection timeout
    CONNECT_TIMEOUT = 30
    # Default Reconnect max delay.
    RECONNECT_MAX_DELAY = 60
    # Default reconnect attempts
    RECONNECT_MAX_TRIES = 50

    # Flag to set if its first connect
    _is_first_connect = True

    # Minimum delay which should be set between retries. User can't set less
    # than this
    _minimum_reconnect_max_delay = 5
    # Maximum number or retries user can set
    _maximum_reconnect_max_tries = 300
    ws_endpoints = {"login": "wss://ws.quantsapp.com"}

    def __init__(self,
                 user_name: str=None,
                 password: str=None,
                 api_key: str=None,
                 access_token: str=None,
                 debug=False,
                 reconnect=True,
                 reconnect_max_tries=RECONNECT_MAX_TRIES,
                 reconnect_max_delay=RECONNECT_MAX_DELAY,
                 connect_timeout=CONNECT_TIMEOUT):
        threading.Thread.__init__(self)
        # Set max reconnect tries
        if reconnect_max_tries > self._maximum_reconnect_max_tries:
            log.warning("`reconnect_max_tries` can not be more than {val}. Setting to highest possible value - {val}.".format(
                val=self._maximum_reconnect_max_tries))
            self.reconnect_max_tries = self._maximum_reconnect_max_tries
        else:
            self.reconnect_max_tries = reconnect_max_tries
        # Set max reconnect delay
        if reconnect_max_delay < self._minimum_reconnect_max_delay:
            log.warning("`reconnect_max_delay` can not be less than {val}. Setting to lowest possible value - {val}.".format(
                val=self._minimum_reconnect_max_delay))
            self.reconnect_max_delay = self._minimum_reconnect_max_delay
        else:
            self.reconnect_max_delay = reconnect_max_delay
        self.connect_timeout = connect_timeout
        # Debug enables logs
        self.debug = debug
        # Placeholders for callbacks.
        self.on_ticks = None
        self.on_open = None
        self.on_close = None
        self.on_error = None
        self.on_connect = None
        self.on_message = None
        self.on_reconnect = None
        self.on_noreconnect = None
        # List of to be subscribed tokens
        self.to_be_subscribed_tokens = []
        # List of current subscribed tokens
        self.subscribed_tokens = []
        self._user_agent = requests.get(
            "https://techfanetechnologies.github.io/latest-user-agent/user_agents.json").json()[-2]
        self._get_app_version()
        self._fetch_master_script()
        self._get_instrument_records()
        self._config = dotenv_values(".env.secret")
        self._user_name = self._config['USER_NAME'] if self._config and len(
            self._config.keys()) != 0 and 'USER_NAME' in self._config.keys() and user_name is None else user_name
        self._password = self._config['PASSWORD'] if self._config and len(
            self._config.keys()) != 0 and 'PASSWORD' in self._config.keys() and password is None else password
        # self._api_key,  self._access_token = api_key, access_token if api_key
        # is not None and access_token is not None else self._auth()
        self._config = dotenv_values(".env")
        self._api_key = self._config['API_KEY'] if self._config and len(
            self._config.keys()) != 0 and 'API_KEY' in self._config.keys() and api_key is None else api_key
        self._access_token = self._config['ACCESS_TOKEN'] if self._config and len(
            self._config.keys()) != 0 and 'ACCESS_TOKEN' in self._config.keys() and access_token is None else access_token
        self._validate_session()
        self.ws_endpoints = self.ws_endpoints | {
            "user": f"wss://wsoc.quantsapp.com/?user_id={self._api_key}&token={self._access_token}&portal=web&version={self._app_version}&country=in",
            "streaming": f"wss://server.quantsapp.com/stream?user_id={self._api_key}&token={self._access_token}&portal=web&version={self._app_version}&country=in&force_login=false"}
        self.socket_url = self.ws_endpoints["streaming"]
        self._connected_event = threading.Event()

    def _get_app_version(self):
        _BASE_URL = "https://web.quantsapp.com"
        s = requests.get(
            _BASE_URL, headers={'User-Agent': self._user_agent})
        mainjs = re.findall(r'main-es2015\.\w+\.js', s.text)
        mainjs = re.findall(
            r'main.*\w+\.js', s.text)[0].split(" ")[0].replace('"', '')
        mainjs = requests.get(f"{_BASE_URL}/{mainjs}", headers={'User-Agent': self._user_agent})
        kiqv = json.loads(re.findall(
            r'kiQV\:function\(t\)\{t\.exports\=JSON\.parse\(\'..*\'\)\}\,kmnG', mainjs.text)[0].split("\'")[1])
        self._app_name, self._app_version, self._app_key = kiqv[
            "name"], kiqv["version"], kiqv["key"]

    def _auth(self):
        from contextlib import closing
        from websocket import create_connection
        print(self._app_version, self._user_name, self._password,
              self.ws_endpoints["login"].lstrip("wss://"), self._user_agent)
        with closing(create_connection(self.ws_endpoints["login"],
                                       origin="https://web.quantsapp.com",
                                       host="ws.quantsapp.com",
                                       header={'User-Agent': self._user_agent})) as self._ws_auth:
            self._ws_auth.send(
                json.dumps({
                    "noti_token": "0",
                    "action": "signin",
                    "mode": "login_custom",
                    "platform": "web",
                    "version": self._app_version,  # "2.3.57",
                    "country": "in",
                    "email": self._user_name,
                    "user_password": self._password,
                    "sub_platform": "live",
                    "source": "qapp"
                }))
            msg = json.loads(self._ws_auth.recv())
            print(msg)
            if msg["status"] != '1' and msg["msg"] != "Login Successful" and msg["routeKey"] != "signin" and msg["custom_key"] != msg["routeKey"]:
                raise ValueError('failed to authenticate')
        with open(".env", "w") as f:
            f.write(f"API_KEY={msg['api_key']}")
            f.write(f"ACCESS_TOKEN={msg['token']}")
        return msg["api_key"], msg["token"]

    def _validate_session(self):
        if not _validate_sessions(self._api_key, self._access_token, self._app_version):
            self._api_key, self._access_token = self._auth()

    def _logout(self):
        try:
            self.ws.sendMessage(
                six.b(
                    json.dumps({
                        "mode": "logout",
                        "custom_key": "logout",
                        "action": "user_profile",
                        "country": "in",
                        "version": self._app_version,  # "2.3.57",
                        "platform": "web",
                        "sub_platform": "live"
                    }))
            )
            return True
        except Exception as e:
            self._close(reason="Error while logout: {}".format(str(e)))
            raise
        # {"status": "1", "msg": "Logged out Successfully.", "routeKey": "user_profile", "custom_key": "logout"}

    def _create_connection(self, url, **kwargs):
        """Create a WebSocket client connection."""
        # print(url)
        self.factory = QTSAppClientFactory(url, **kwargs)

        # Alias for current websocket connection
        self.ws = self.factory.ws

        self.factory.debug = self.debug

        # Register private callbacks
        self.factory.on_open = self._on_open
        self.factory.on_error = self._on_error
        self.factory.on_close = self._on_close
        self.factory.on_message = self._on_message
        self.factory.on_connect = self._on_connect
        self.factory.on_reconnect = self._on_reconnect
        self.factory.on_noreconnect = self._on_noreconnect

        self.factory.maxDelay = self.reconnect_max_delay
        self.factory.maxRetries = self.reconnect_max_tries

    def connect(self, threaded=True, disable_ssl_verification=False, proxy=None):
        """
        Establish a websocket connection.

        - `threaded` is a boolean indicating if the websocket client has to be run in threaded mode or not
        - `disable_ssl_verification` disables building ssl context
        - `proxy` is a dictionary with keys `host` and `port` which denotes the proxy settings
        """
        # Custom headers
        headers = {
            'Origin': 'https://web.quantsapp.com',
            'Host': "server.quantsapp.com"
        }

        # Init WebSocket client factory
        self._create_connection(self.socket_url,
                                useragent=self._user_agent,
                                proxy=proxy, headers=headers)

        # Set SSL context
        context_factory = None
        if self.factory.isSecure and not disable_ssl_verification:
            context_factory = ssl.ClientContextFactory()

        # Establish WebSocket connection to a server
        connectWS(self.factory, contextFactory=context_factory,
                  timeout=self.connect_timeout)

        if self.debug:
            twisted_log.startLogging(sys.stdout)

        # Run in seperate thread of blocking
        opts = {}

        # Run when reactor is not running
        if not reactor.running:
            if threaded:
                # Signals are not allowed in non main thread by twisted so
                # suppress it.
                opts["installSignalHandlers"] = False
                self.websocket_thread = threading.Thread(
                    target=reactor.run, kwargs=opts)
                self.websocket_thread.daemon = True
                self.websocket_thread.start()
            else:
                reactor.run(**opts)

    def is_connected(self):
        """Check if WebSocket connection is established."""
        if self.ws and self.ws.state == self.ws.STATE_OPEN:
            return True
        else:
            return False

    def _close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        if self.ws:
            self.ws.sendClose(code, reason)

    def close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        self._logout()
        self.stop_retry()
        self._close(code, reason)

    def stop(self):
        """Stop the event loop. Should be used if main thread has to be closed in `on_close` method.
        Reconnection mechanism cannot happen past this method
        """
        reactor.stop()

    def stop_retry(self):
        """Stop auto retry when it is in progress."""
        if self.factory:
            self.factory.stopTrying()

    def subscribe(self, _payload, _isBinary=True):
        """
        Subscribe to a list of instrument_tokens.

        - `instrument_tokens` is list of instrument instrument_tokens to subscribe
        """
        self.ws.sendMessage(_payload, _isBinary)
        return True

    def _send_stream_subscribe_request(self, instrument, expiry):
        self._populate_oc_table_data(instrument, expiry)
        if isinstance(expiry, dtdt):
            expiry = expiry.strftime("%d%m%Y")
        _instrument = f"{instrument.upper()}:{expiry}"
        if self._isNowInTimePeriod(dt.time(9, 15), dt.time(15, 30), dt.datetime.now().time()):  # noqa: E501
            print(f"----Sending Subscribe Stream Request for {_instrument}----")  # noqa: E501
            self.subscribe(
                self._subscribe_packets_formated(instrument, expiry))
        else:
            if self.__isNowInTimePeriod(dt.time(0, 0), dt.time(9, 15), dt.datetime.now().time()):  # noqa: E501
                print(f"Sending Subscribe Stream Request for {_instrument} failed due to streaming attempt pre market hours ---")  # noqa: E501
            elif self.__isNowInTimePeriod(dt.time(15, 30), dt.time(23, 59), dt.datetime.now().time()):  # noqa: E501
                print(f"Sending Subscribe Stream Request for {_instrument} failed due to streaming attempt post market hours  ---")  # noqa: E501
            else:
                print(f"Sending Subscribe Stream Request for {_instrument} failed !!!")  # noqa: E501

    def _send_stream_unsubscribe_request(self, instrument, expiry):
        if isinstance(expiry, dtdt):
            expiry = expiry.strftime("%d%m%Y")
        _instrument = f"{instrument.upper()}:{expiry}"
        print(
            f"---Sending UnSubscribe Stream Request for {_instrument}----")  # noqa: E501
        self.subscribe(self._unsubscribe_packets_formated(instrument, expiry))

    def _resubscribe_on_instrument_change(self):
        instrument, expiry = self._ws_ir[
            "J3"].value, self._ws_ir["K3"].value
        if isinstance(expiry, dtdt):
            _expiry = expiry.strftime("%d%m%Y")
        _instrument = f"{instrument.upper()}:{_expiry}"
        print(self.subscribed_tokens[0], _instrument)
        if (self.subscribed_tokens[0] != _instrument):
            if self.debug:
                log.debug(
                    "Resubscribe: {} - {}".format(instrument, expiry))
            prev_instrument, prev_expiry = self.subscribed_tokens[
                0].split(":")[0], self.subscribed_tokens[0].split(":")[-1]
            self._send_stream_unsubscribe_request(prev_instrument, prev_expiry)
            self.to_be_subscribed_tokens.append([instrument, expiry])
            self._send_stream_subscribe_request(instrument, expiry)

    def resubscribe_on_instrument_change(self):
        if self._connected_event.wait(timeout=10):
            reactor.callFromThread(self._resubscribe_on_instrument_change)

    def _on_connect(self, ws, response):
        self.ws = ws
        if self.on_connect:
            self.on_connect(self, response)

    def _on_close(self, ws, code, reason):
        """Call `on_close` callback when connection is closed."""
        log.error("Connection closed: {} - {}".format(code, str(reason)))

        if self.on_close:
            self.on_close(self, code, reason)

    def _on_error(self, ws, code, reason):
        """Call `on_error` callback when connection throws an error."""
        log.error("Connection error: {} - {}".format(code, str(reason)))

        if self.on_error:
            self.on_error(self, code, reason)

    def _on_message(self, ws, payload, is_binary):
        """Call `on_message` callback when text message is received."""
        if self.on_message:
            self.on_message(self, payload, is_binary)

        # If the message is binary, parse it and send it to the callback.
        # if self.on_ticks and is_binary and len(payload) > 4:
        #     self.on_ticks(self, self._parse_binary(payload))

        if is_binary and len(payload) > 4:
            self._decode_packets(payload)

    def _on_open(self, ws):
        # self._on_init_get_option_chain()
        # Resubscribe if its reconnect
        if not self._is_first_connect:
            self.resubscribe_on_instrument_change()

        # Set first connect to false once its connected first time
        self._is_first_connect = False

        if self.on_open:
            return self.on_open(self)

    def _on_reconnect(self, attempts_count):
        if self.on_reconnect:
            return self.on_reconnect(self, attempts_count)

    def _on_noreconnect(self):
        if self.on_noreconnect:
            return self.on_noreconnect(self)

    def _fetch_master_script(self):
        self._master_script = requests.get(
            "https://techfanetechnologies.github.io/QtSAppMasterScript/masterScript.json").json()  # noqa: E501
        self._instruments = self._master_script.keys()

    def _get_instrument_records(self):
        _instrument_records = [
            ["SymbolName", "ExpiryDate", "LotSize", "Strikes"]]
        for _symbol in self._instruments:
            _instrument_records.extend([[_symbol, self._master_script[_symbol]["expiry"][_idx], lot, len(  # noqa: E501
                self._master_script[_symbol]["strikes"][0])] for _idx, lot in enumerate(self._master_script[_symbol]["lot"])])  # noqa: E501
        self._instrument_records = pd.DataFrame(_instrument_records)

    def _get_lot_value(self, _symbol: str, _expiry: str):
        if _symbol.upper() in self._instruents and _expiry in self._master_script[_symbol]["expiry"]:  # noqa: E501
            return self._master_script[_symbol]["lot"][self._master_script[_symbol]["expiry"].index(_expiry)]  # noqa: E501
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_expiry_dates(self, _symbol: str):
        if _symbol.upper() in self._instruents:
            return self._master_script[_symbol]["expiry"]
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_list_of_strikes(self, _symbol: str, _expiry: str):
        if _symbol.upper() in self._instruents and _expiry in self._master_script[_symbol]["expiry"]:  # noqa: E501
            return self._master_script[_symbol]["strikes"][self._master_script[_symbol]["expiry"].index(_expiry)]  # noqa: E501
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_atm_strike(self, _symbol: str, _expiry: str, _ltp: float):
        all_strikes = self._get_list_of_strikes(_symbol, _expiry)
        atm_strike = int(min(all_strikes, key=lambda x: abs(x - _ltp)))
        return atm_strike

    def _find_atm_strike(self, _all_strikes: pd.core.series.Series, _ltp: float):  # noqa: E501
        return int(min(_all_strikes.astype(int).tolist(), key=lambda x: abs(x - _ltp)))  # noqa: E501

    def _set_df_column_value_at_index(self, _index: int, _column_name: str, _value, _fast=True):  # noqa: E501
        if _fast:
            self._df.at[_index, _column_name] = _value
        else:
            self._df.iloc[
                _index, self._df.columns.get_loc(_column_name)] = _value

    def _get_df_column_value_at_index(self, _index: int, _column_name: str, _fast=True):  # noqa: E501
        if _fast:
            return self._df.at[_index, _column_name]
        else:
            return self._df.iloc[_index, self._df.columns.get_loc(_column_name)]  # noqa: E501

    def _is_fut_or_fair_price(self, price: list):
        future_place = price[-1]
        if future_place == 0:
            return "FuturePrice"
        elif future_place == 1:
            return "FairPrice"
        else:
            return "Unknown"

    def _is_time_between(self, begin_time, end_time, check_time=None):
        from datetime import datetime, time
        # If check time is not given, default to current UTC time
        check_time = check_time or datetime.utcnow().time()
        if begin_time < end_time:
            return check_time >= begin_time and check_time <= end_time
        else:  # crosses midnight
            return check_time >= begin_time or check_time <= end_time

    def _isNowInTimePeriod(self, startTime, endTime, nowTime):
        if startTime < endTime:
            return nowTime >= startTime and nowTime <= endTime
        else:
            # Over midnight:
            return nowTime >= startTime or nowTime <= endTime

    def _populate_oc_table_data(self, _instrument, _expiry):
        from contextlib import closing
        from websocket import create_connection
        if isinstance(_expiry, dtdt):
            _expiry = _expiry.strftime("%d-%b-%y")
        with closing(create_connection(self.ws_endpoints["user"],
                                       origin="https://web.quantsapp.com",
                                       host="wsoc.quantsapp.com",
                                       header={'User-Agent': self._user_agent})) as self._ws_user:
            self._ws_user.send(
                json.dumps({
                    "Scrip": _instrument,  # "NIFTY",
                    "Expiry": _expiry,  # "07-Jul-22",
                    "custom_key": "chain",
                    "action": "chain-pain-skew-pcr",
                    "platform": "web",
                    "version": self._app_version,  # "2.3.57",
                    "sub_platform": "live"
                }))
            msg = json.loads(self._ws_user.recv())
            print(msg)
            if msg["status"] != '1' and msg["msg"] != "success" and msg["custom_key"] != "chain" and msg["routeKey"] != "chain-pain-skew-pcr":
                raise ValueError(f'failed to fetch oc_table, Error {msg}')
            elif msg["status"] == "1" and msg["msg"] == "success" and msg["statusCode"] == 200 and msg["custom_key"] == "chain" and msg["routeKey"] == "chain-pain-skew-pcr":  # noqa: E501
                timestamp = msg["timestamp"]
                strike_list = list(map(int, msg["strike"].split(",")))
                strike_list_length_int_array = [int(0)] * len(strike_list)
                strike_list_length_str_array = [str(0)] * len(strike_list)
                strike_list_length_float_array = [float(0)] * len(strike_list)
                oc_table = {
                    "PutClose":
                        strike_list_length_float_array,
                    "PutLow":
                        strike_list_length_float_array,
                    "PutHigh":
                        strike_list_length_float_array,
                    "PutOpen":
                        strike_list_length_float_array,
                    "PutVetaChange":
                        strike_list_length_float_array,
                    "PutVeta":
                        strike_list_length_float_array,
                    "PutVolgaChange":
                        strike_list_length_float_array,
                    "PutVolga":
                        strike_list_length_float_array,
                    "PutColorChange":
                        strike_list_length_float_array,
                    "PutColor":
                        strike_list_length_float_array,
                    "PutZommaChange":
                        strike_list_length_float_array,
                    "PutZomma":
                        strike_list_length_float_array,
                    "PutSpeedChange":
                        strike_list_length_float_array,
                    "PutSpeed":
                        strike_list_length_float_array,
                    "PutCharmChange":
                        strike_list_length_float_array,
                    "PutCharm":
                        strike_list_length_float_array,
                    "PutVannaChange":
                        strike_list_length_float_array,
                    "PutVanna":
                        strike_list_length_float_array,
                    "PutGammaChange":
                        strike_list_length_float_array,
                    "PutGamma":
                        list(map(float, msg["p_gamma"].split(","))),
                    "PutVegaChange":
                        strike_list_length_float_array,
                    "PutVega":
                        list(map(float, msg["p_vega"].split(","))),
                    "PutThetaChange":
                        strike_list_length_float_array,
                    "PutTheta":
                        list(map(float, msg["p_theta"].split(","))),
                    "PutDeltaChange":
                        strike_list_length_float_array,
                    "PutDelta":
                        list(map(float, msg["p_delta"].split(","))),
                    "PutPrevIV":
                        list(map(float, msg["p_prev_iv"].split(","))),
                    "PutVolume":
                        list(map(int, msg["p_volume"].split(","))),
                    "PutOIChange":
                        list(map(int, msg["p_oi_change"].split(","))),
                    "PutOI":
                        list(map(int, msg["p_oi"].split(","))),
                    "PutIV":
                        list(map(float, msg["p_iv"].split(","))),
                    "PutLtpChange":
                        list(map(float, msg["p_ltp_change"].split(","))),
                    "PutLtp":
                        list(map(float, msg["p_ltp"].split(","))),
                    "StrikePrice":
                        strike_list,
                    "CallLtp":
                        list(map(float, msg["c_ltp"].split(","))),
                    "CallLtpChange":
                        list(map(float, msg["c_ltp_change"].split(","))),
                    "CallIV":
                        list(map(float, msg["c_iv"].split(","))),
                    "CallOI":
                        list(map(int, msg["c_oi"].split(","))),
                    "CallOIChange":
                        list(map(int, msg["c_oi_change"].split(","))),
                    "CallVolume":
                        list(map(int, msg["c_volume"].split(","))),
                    "CallDelta":
                        list(map(float, msg["c_delta"].split(","))),
                    "CallDeltaChange":
                        strike_list_length_float_array,
                    "CallTheta":
                        list(map(float, msg["c_theta"].split(","))),
                    "CallThetaChange":
                        strike_list_length_float_array,
                    "CallVega":
                        list(map(float, msg["c_vega"].split(","))),
                    "CallVegaChange":
                        strike_list_length_float_array,
                    "CallGamma":
                        list(map(float, msg["c_gamma"].split(","))),
                    "CallGammaChange":
                        strike_list_length_float_array,
                    "CallPrevIV":
                        list(map(float, msg["c_prev_iv"].split(","))),
                    "CallVanna":
                        strike_list_length_float_array,
                    "CallVannaChange":
                        strike_list_length_float_array,
                    "CallCharm":
                        strike_list_length_float_array,
                    "CallCharmChange":
                        strike_list_length_float_array,
                    "CallSpeed":
                        strike_list_length_float_array,
                    "CallSpeedChange":
                        strike_list_length_float_array,
                    "CallZomma":
                        strike_list_length_float_array,
                    "CallZommaChange":
                        strike_list_length_float_array,
                    "CallColor":
                        strike_list_length_float_array,
                    "CallColorChange":
                        strike_list_length_float_array,
                    "CallVolga":
                        strike_list_length_float_array,
                    "CallVolgaChange":
                        strike_list_length_float_array,
                    "CallVeta":
                        strike_list_length_float_array,
                    "CallVetaChange":
                        strike_list_length_float_array,
                    "CallOpen":
                        strike_list_length_float_array,
                    "CallHigh":
                        strike_list_length_float_array,
                    "CallLow":
                        strike_list_length_float_array,
                    "CallClose":
                        strike_list_length_float_array,
                    "CallAskPrice":
                        strike_list_length_float_array,
                    "CallAskQty":
                        strike_list_length_int_array,
                    "CallBidPrice":
                        strike_list_length_float_array,
                    "CallBidQty":
                        strike_list_length_int_array,
                    "CallAveragePrice":
                        strike_list_length_float_array,
                    "PutAskPrice":
                        strike_list_length_float_array,
                    "PutAskQty":
                        strike_list_length_int_array,
                    "PutBidPrice":
                        strike_list_length_float_array,
                    "PutBidQty":
                        strike_list_length_int_array,
                    "PutAveragePrice":
                        strike_list_length_float_array,
                    "CallDataUpdateTimeStamp":
                        list(map(lambda x: timestamp, strike_list_length_str_array)),  # noqa: E501
                    "PutDataUpdateTimeStamp":
                        list(map(lambda x: timestamp, strike_list_length_str_array)),  # noqa: E501
                    "FuturePrice":
                        strike_list_length_float_array,
                    "FutureOrFairPrice":
                        strike_list_length_str_array,
                    "FutureDataUpdateTimeStamp":
                        strike_list_length_str_array,
                }
                futureprice = float(msg["rp"].split(",")[0])
                futureorfairprice = "FuturePrice" if msg["rp"].split(
                    ",")[1] == '0' else "FairPrice" if msg["rp"].split(",")[1] == '1' else "Unknown"  # noqa: E501
                futuredataupdatetimestamp = msg["timestamp"]
                self._df = pd.DataFrame.from_dict(oc_table)
                self._atm_strike = msg["astrike"] if self._find_atm_strike(self._df["StrikePrice"], futureprice) == msg[  # noqa: E501
                    "astrike"] else self._find_atm_strike(self._df["StrikePrice"], futureprice)  # noqa: E501
                self._df.eval("PutIVChange = PutIV - PutPrevIV", inplace=True)
                self._df.eval(
                    "PutIVChangePercent = PutIVChange / PutPrevIV", inplace=True)  # noqa: E501
                self._df.eval("PutPrevOI = PutOI - PutOIChange", inplace=True)
                self._df.eval(
                    "PutOIChangePercent = PutOIChange / PutPrevOI", inplace=True)  # noqa: E501
                self._df.eval(
                    "PutPrevLTP = PutLtp - PutLtpChange", inplace=True)
                self._df.eval(
                    "PutLtpChangePercent = PutLtpChange / PutPrevLTP", inplace=True)  # noqa: E501
                self._df.eval(
                    "CallPrevLTP = CallLtp - CallLtpChange", inplace=True)
                self._df.eval(
                    "CallLtpChangePercent = CallLtpChange / CallPrevLTP", inplace=True)  # noqa: E501
                self._df.eval(
                    "CallPrevOI = CallOI - CallOIChange", inplace=True)
                self._df.eval(
                    "CallOIChangePercent = CallOIChange / CallPrevOI", inplace=True)  # noqa: E501
                self._df.eval(
                    "CallIVChange = CallIV - CallPrevIV", inplace=True)
                self._df.eval(
                    "CallIVChangePercent = CallIVChange / CallPrevIV", inplace=True)  # noqa: E501
                self._df.eval("immediatePrevCLtp = CallPrevLTP", inplace=True)
                self._df.eval("immediatePrevPLtp = PutPrevLTP", inplace=True)
                self._df.eval("immediatePrevFLtp = FuturePrice", inplace=True)
                self._df.insert(97, "CLtpColor", np.select(((self._df["CallPrevLTP"] - self._df["CallLtp"]) > 0, (self._df[  # noqa: E501
                          "CallPrevLTP"] - self._df["CallLtp"]) < 0), ("reself._dfontColor", "greenFontColor"), "whiteFontColor"))  # noqa: E501
                self._df.insert(98, "PLtpColor", np.select(((self._df["PutPrevLTP"] - self._df["PutLtp"]) > 0, (self._df[  # noqa: E501
                          "PutPrevLTP"] - self._df["PutLtp"]) < 0), ("reself._dfontColor", "greenFontColor"), "whiteFontColor"))  # noqa: E501
                self._df.insert(99, "FLtpColor", np.select(((self._df["immediatePrevFLtp"] - self._df["FuturePrice"]) > 0, (self._df[  # noqa: E501
                          "immediatePrevFLtp"] - self._df["FuturePrice"]) < 0), ("reself._dfontColor", "greenFontColor"), "whiteFontColor"))  # noqa: E501
                self._set_df_column_value_at_index(
                    0, "FuturePrice", futureprice)
                self._set_df_column_value_at_index(
                    0, "FutureOrFairPrice", futureorfairprice)
                self._set_df_column_value_at_index(
                    0, "FutureDataUpdateTimeStamp", futuredataupdatetimestamp)
                self._maxcalloi, self._maxcalloi_idx = self._df["CallOI"].astype(  # noqa: E501
                    int).max(), self._df["CallOI"].astype(int).idxmax()
                self._maxcalloi_strike = self._df[
                    "StrikePrice"].iloc[self._maxcalloi_idx]
                self._maxputoi, self._maxputoi_idx = self._df["PutOI"].astype(  # noqa: E501
                    int).max(), self._df["PutOI"].astype(int).idxmax()
                self._maxputoi_strike = self._df[
                    "StrikePrice"].iloc[self._maxputoi_idx]
                self._update_option_chain_in_excel_wb()

    def _check_n_return_file_path(self, _file_path):
        try:
            if ((os.path.exists(os.path.join(os.getcwd(), _file_path))
                    and os.path.isfile(os.path.join(os.getcwd(), _file_path)))
                    and not os.path.isdir(os.path.join(os.getcwd(), _file_path))):  # noqa: E501
                _file_path = os.path.join(os.getcwd(), _file_path)
                return _file_path
            else:
                print(f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again")  # noqa: E501
                raise ValueError(f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again")  # noqa: E501
        except ValueError:
            raise ValueError(f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again")  # noqa: E501

    def _update_excel_wb(self, _wb_name: str=None):
        _wb_name = "OptionChain" if not _wb_name else _wb_name
        self._wb = xw.Book(
            self._check_n_return_file_path(f"{_wb_name}.xlsx"))
        self._ws_ir = self._wb.sheets["IR"]
        self._ws_rawdata = self._wb.sheets["RawData"]
        self._ws_ir["A1"].options(pd.DataFrame, index=False, header=False,
                                  expand='table').value = self._instrument_records

    def _update_option_chain_in_excel_wb(self, _streaming=False):
        # while True:
        self._ws_rawdata["C1"].options(
            pd.DataFrame, index=True, header=True, expand='table').value = self._df  # noqa: E501
        if _streaming:
            self._atm_strike = self._find_atm_strike(
                self._df["StrikePrice"], self._get_df_column_value_at_index(0, "FuturePrice"))  # noqa: E501
            self._maxcalloi, self._maxcalloi_idx = self._df["CallOI"].astype(  # noqa: E501
                int).max(), self._df["CallOI"].astype(int).idxmax()
            self._maxcalloi_strike = self._df[
                "StrikePrice"].iloc[self._maxcalloi_idx]
            self._maxputoi, self._maxputoi_idx = self._df["PutOI"].astype(  # noqa: E501
                int).max(), self._df["PutOI"].astype(int).idxmax()
            self._maxputoi_strike = self._df[
                "StrikePrice"].iloc[self._maxputoi_idx]
        self._ws_rawdata["A1"].value, self._ws_rawdata["A2"].value, self._ws_rawdata["A3"].value, self._ws_rawdata["A4"].value, self._ws_rawdata["A5"].value, self._ws_rawdata[  # noqa: E501
            "A6"].value, self._ws_rawdata["A7"].value = "ATMStrike", "MaxCallOI", "MaxCallOIIndex", "MaxCallOIStrike", "MaxPutOI", "MaxPutOIIndex", "MaxPutOIStrike"  # noqa: E501

        self._ws_rawdata["B1"].value, self._ws_rawdata["B2"].value, self._ws_rawdata["B3"].value, self._ws_rawdata["B4"].value, self._ws_rawdata["B5"].value, self._ws_rawdata[  # noqa: E501
            "B6"].value, self._ws_rawdata["B7"].value = self._atm_strike, self._maxcalloi, self._maxcalloi_idx, self._maxcalloi_strike, self._maxputoi, self._maxputoi_idx, self._maxputoi_strike  # noqa: E501

    def _on_init_get_option_chain(self, _symbol: str=None, _expiry: str=None):
        self._update_excel_wb()
        if not _symbol and not _expiry:
            _symbol = self._ws_ir["J3"].value
            _expiry = self._ws_ir["K3"].value
        _instrument = f'{_symbol}:{_expiry.strftime("%d%m%Y")}'
        print(f'{_instrument}')
        self.to_be_subscribed_tokens.append([_symbol, _expiry])
        self._send_stream_subscribe_request(_symbol, _expiry)

    def _get_length(self, _byte_format: list):
        if len(_byte_format) >= 2:
            return sum(list(map(lambda x: sum(list(map(
                lambda x: self.FORMAT_CHARACTERS[x]['Standard_Size'] if x not in ['s', 'x', 'p', 'P'] else 0, x))  # noqa: E501
                    ) if isinstance(x, str) else x, _byte_format)))  # noqa: E501
        else:
            return sum(list(map(
                        lambda x: self.FORMAT_CHARACTERS[x]['Standard_Size'] if x not in ['s', 'x', 'p', 'P'] else 0, _byte_format[0])))  # noqa: E501

    def _unpack(self, _bin, _end, _start=0, _byte_format="ch"):
        """Unpack binary data as unsgined interger."""
        return struct.unpack("<" + _byte_format, _bin[_start:_end])

    def _pack(self, _bin, _byte_format="ch"):
        """Unpack binary data as unsgined interger."""
        return struct.pack("<" + _byte_format, _bin)

    def _decode_packets(self, _bin):
        try:
            if len(_bin) >= 2:
                j = self._get_length(_byte_format=["ch"])
                initial_data = self._unpack(_bin, _end=j, _byte_format="ch")
                message_type, string_length = initial_data[
                    0].decode('utf-8'), initial_data[1]
                print(message_type, string_length)
                if "m" == message_type:
                    j = self._get_length(_byte_format=["chc"])
                    t = self._unpack(_bin, _end=j, _byte_format="chc")
                    if t[-1] == "s":
                        _symbol, _expiry = self.to_be_subscribed_tokens[
                            0][0], self.to_be_subscribed_tokens[0][-1]
                        self.subscribed_tokens.append(f"{_symbol}:{_expiry}")  # noqa: E501
                        self.to_be_subscribed_tokens = []
                        print(f"-- {_symbol}:{_expiry} Subscribed Successfully --")  # noqa: E501
                    elif t[-1] == "u":
                        _symbol, _expiry = self.subscribed_tokens[
                            0].split(":")[0], self.subscribed_tokens[0].split(":")[-1]
                        self.subscribed_tokens.remove(f"{_symbol}:{_expiry}")  # noqa: E501
                        print(f"-- {_symbol}:{_expiry} Unsubscribed Successfully --")  # noqa: E501
                    else:
                        print(f"-- {t[-1]} --")
                elif "c" == message_type:
                    j = self._get_length(_byte_format=["chc"])
                    return self._unpack(_bin, _end=j, _byte_format="chc")
                else:
                    _symbol, _expiry = self.to_be_subscribed_tokens[
                        0][0], self.to_be_subscribed_tokens[0][-1]
                    _byte_format = ["chh", string_length, "s"]
                    j = self._get_length(_byte_format)
                    t = self._unpack(
                        _bin, _end=j, _byte_format="".join(list(map(str, _byte_format))))  # noqa: E501
                    t = t[3].decode('utf-8')
                    # print(t)
                    symbol_expiry_and_cepe_strike_or_future = t.split("|")
                    symbol_expiry = symbol_expiry_and_cepe_strike_or_future[0]
                    symbol, expiry = symbol_expiry.split(
                        ":")[0], symbol_expiry.split(":")[-1]
                    cepe_strike_or_future = symbol_expiry_and_cepe_strike_or_future[
                        -1] if symbol_expiry_and_cepe_strike_or_future[-1] else ""
                    if "x" != cepe_strike_or_future and "" != cepe_strike_or_future:  # noqa: E501
                        cepe, strike = cepe_strike_or_future.split(
                            ":")[0], cepe_strike_or_future.split(":")[-1]
                    # print(symbol_expiry, cepe_strike_or_future)
                if ("g" == message_type and "x" == cepe_strike_or_future
                        and symbol == _symbol and expiry == _expiry.strftime("%d%m%Y")):  # noqa: E501
                    _byte_format = ["chh", string_length, "siffffqfififqc"]
                    j = self._get_length(_byte_format)
                    t = self._unpack(
                        _bin, _end=j, _byte_format="".join(list(map(str, _byte_format))))  # noqa: E501
                    print(t)
                    if len(symbol_expiry_and_cepe_strike_or_future) == 2:
                        self._set_df_column_value_at_index(
                            0, "FutureDataUpdateTimeStamp", self._qts_app_ts_decode(+t[4], _string=True))  # noqa: E501
                        self._set_df_column_value_at_index(
                            0, "FuturePrice", +t[8])
                        self._set_df_column_value_at_index(0, "FLtpColor", "reself._dfontColor" if (  # noqa: E501
                            self._get_df_column_value_at_index(0, "immediatePrevFLtp") - +t[8]) > 0 else "greenFontColor" if (  # noqa: E501
                            self._get_df_column_value_at_index(0, "immediatePrevFLtp") - +t[8]) < 0 else "whiteFontColor")  # noqa: E501
                        self._set_df_column_value_at_index(
                            0, "immediatePrevFLtp", +t[8])
                        self._update_option_chain_in_excel_wb()
                elif ("g" == message_type and "x" != cepe_strike_or_future and "" != cepe_strike_or_future  # noqa: E501
                        and symbol == _symbol and expiry == _expiry.strftime("%d%m%Y")):  # noqa: E501
                    print("dataPriceWithoutStrike -- without strike --")
                    _byte_format = [
                        "chh", string_length, "siffffqfififqffffffffffff"]
                    j = self._get_length(_byte_format)
                    t = self._unpack(
                        _bin, _end=j, _byte_format="".join(list(map(str, _byte_format))))  # noqa: E501
                    print(t)
                    if len(symbol_expiry_and_cepe_strike_or_future) == 2:
                        n = list(
                            np.where(self._df["StrikePrice"] == strike))[0]
                        print(n)
                        if "c" == cepe:
                            self._set_df_column_value_at_index(
                                n, "CallDataUpdateTimeStamp", self._qts_app_ts_decode(+t[4], _string=True))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallOpen", +t[5])
                            self._set_df_column_value_at_index(
                                n, "CallHigh", +t[6])
                            self._set_df_column_value_at_index(
                                n, "CallLow", +t[7])
                            self._set_df_column_value_at_index(
                                n, "CallClose", +t[8])
                            self._set_df_column_value_at_index(
                                n, "CallLtp", +t[8])
                            self._set_df_column_value_at_index(
                                n, "CallLtpChange", -1 * (self._get_df_column_value_at_index(n, "CallPrevLTP") - +t[8]))  # noqa: E501
                            self._set_df_column_value_at_index(n, "CallLtpChange%", (+t[8] - self._get_df_column_value_at_index(  # noqa: E501
                                n, "CallPrevLTP")) / self._get_df_column_value_at_index(n, "CallPrevLTP"))  # noqa: E501
                            self._set_df_column_value_at_index(n, "CLtpColor", "reself._dfontColor" if (self._get_df_column_value_at_index(n, "immediatePrevCLtp") - +t[8]) > 0 else "greenFontColor" if (self._get_df_column_value_at_index(n, "immediatePrevCLtp") - +t[8]) < 0 else "whiteFontColor")  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "immediatePrevCLtp", +t[8])
                            self._set_df_column_value_at_index(
                                n, "CallPrevLTP", +t[8])
                            self._set_df_column_value_at_index(
                                n, "CallVolume", +t[9])
                            self._set_df_column_value_at_index(
                                n, "CallAskPrice", +t[10])
                            self._set_df_column_value_at_index(
                                n, "CallAskQty", +t[11])
                            self._set_df_column_value_at_index(
                                n, "CallBidPrice", +t[12])
                            self._set_df_column_value_at_index(
                                n, "CallBidQty", +t[13])
                            self._set_df_column_value_at_index(
                                n, "CallAveragePrice", +t[14])
                            self._set_df_column_value_at_index(
                                n, "CallOI", +t[15])
                            self._set_df_column_value_at_index(
                                n, "CallOIChange", -1 * (self._get_df_column_value_at_index(n, "CallPrevOI") - +t[15]))  # noqa: E501
                            self._set_df_column_value_at_index(n, "CallOIChange%", (+t[15] - self._get_df_column_value_at_index(  # noqa: E501
                                n, "CallPrevOI")) / self._get_df_column_value_at_index(n, "CallPrevOI"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallPrevOI", +t[15])
                            self._set_df_column_value_at_index(
                                n, "CallDeltaChange", +t[16] - self._get_df_column_value_at_index(n, "CallDelta"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallDelta", +t[16])
                            self._set_df_column_value_at_index(
                                n, "CallThetaChange", +t[17] - self._get_df_column_value_at_index(n, "CallTheta"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallTheta", +t[17])
                            self._set_df_column_value_at_index(
                                n, "CallVegaChange", +t[18] - self._get_df_column_value_at_index(n, "CallVega"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallVega", +t[18])
                            self._set_df_column_value_at_index(
                                n, "CallGammaChange", +t[19] - self._get_df_column_value_at_index(n, "CallGamma"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallGamma", +t[19])
                            self._set_df_column_value_at_index(
                                n, "CallIV", 100 * +t[20])
                            self._set_df_column_value_at_index(
                                n, "CallIVChange",  +(100 * +t[20]) - self._get_df_column_value_at_index(n, "CallPrevIV"))  # noqa: E501
                            self._set_df_column_value_at_index(n, "CallIVChange%", (+(100 * +t[20]) - self._get_df_column_value_at_index(  # noqa: E501
                                n, "CallPrevIV")) / self._get_df_column_value_at_index(n, "CallPrevIV"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallPrevIV", 100 * +t[20])
                            self._set_df_column_value_at_index(
                                n, "CallVannaChange", +t[21] - self._get_df_column_value_at_index(n, "CallVanna"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallVanna", +t[21])
                            self._set_df_column_value_at_index(
                                n, "CallCharmChange", +t[22] - self._get_df_column_value_at_index(n, "CallCharm"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallCharm", +t[22])
                            self._set_df_column_value_at_index(
                                n, "CallSpeedChange", +t[23] - self._get_df_column_value_at_index(n, "CallSpeed"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallSpeed", +t[23])
                            self._set_df_column_value_at_index(
                                n, "CallZommaChange", +t[24] - self._get_df_column_value_at_index(n, "CallZomma"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallZomma", +t[24])
                            self._set_df_column_value_at_index(
                                n, "CallColorChange", +t[25] - self._get_df_column_value_at_index(n, "CallColor"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallColor", +t[25])
                            self._set_df_column_value_at_index(
                                n, "CallVolgaChange", +t[26] - self._get_df_column_value_at_index(n, "CallVolga"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallVolga", +t[26])
                            self._set_df_column_value_at_index(
                                n, "CallVetaChange", +t[27] - self._get_df_column_value_at_index(n, "CallVeta"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "CallVeta", +t[27])
                        elif "p" == cepe:
                            self._set_df_column_value_at_index(
                                n, "PutDataUpdateTimeStamp", self._qts_app_ts_decode(+t[4], _string=True))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutOpen", +t[5])
                            self._set_df_column_value_at_index(
                                n, "PutHigh", +t[6])
                            self._set_df_column_value_at_index(
                                n, "PutLow", +t[7])
                            self._set_df_column_value_at_index(
                                n, "PutClose", +t[8])
                            self._set_df_column_value_at_index(
                                n, "PutLtp", +t[8])
                            self._set_df_column_value_at_index(n, "PutLtpChange", -1 *  # noqa: E501
                                                                (self._get_df_column_value_at_index(n, "PutPrevLTP") - +t[8]))  # noqa: E127, E501
                            self._set_df_column_value_at_index(n, "PutLtpChange%", (+t[8] - self._get_df_column_value_at_index(  # noqa: E501
                                n, "PutPrevLTP")) / self._get_df_column_value_at_index(n, "PutPrevLTP"))  # noqa: E501
                            self._set_df_column_value_at_index(n, "PLtpColor", "reself._dfontColor" if (self._get_df_column_value_at_index(  # noqa: E501
                                n, "immediatePrevCLtp") - +t[8]) > 0 else "greenFontColor" if (self._get_df_column_value_at_index(n, "immediatePrevCLtp") - +t[8]) < 0 else "whiteFontColor")  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "immediatePrevPLtp", +t[8])
                            self._set_df_column_value_at_index(
                                n, "PutPrevLTP", +t[8])
                            self._set_df_column_value_at_index(
                                n, "PutVolume", +t[9])
                            self._set_df_column_value_at_index(
                                n, "PutAskPrice", +t[10])
                            self._set_df_column_value_at_index(
                                n, "PutAskQty", +t[11])
                            self._set_df_column_value_at_index(
                                n, "PutBidPrice", +t[12])
                            self._set_df_column_value_at_index(
                                n, "PutBidQty", +t[13])
                            self._set_df_column_value_at_index(
                                n, "PutAveragePrice", +t[14])
                            self._set_df_column_value_at_index(
                                n, "PutOI", +t[15])
                            self._set_df_column_value_at_index(
                                n, "PutOIChange", -1 * (self._get_df_column_value_at_index(n, "PutPrevOI") - +t[15]))  # noqa: E501
                            self._set_df_column_value_at_index(n, "PutOIChange%", (+t[15] - self._get_df_column_value_at_index(  # noqa: E501
                                n, "PutPrevOI")) / self._get_df_column_value_at_index(n, "PutPrevOI"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutPrevOI", +t[15])
                            self._set_df_column_value_at_index(
                                n, "PutDeltaChange", +t[16] - self._get_df_column_value_at_index(n, "PutDelta"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutDelta", +t[16])
                            self._set_df_column_value_at_index(
                                n, "PutThetaChange", +t[17] - self._get_df_column_value_at_index(n, "PutTheta"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutTheta", +t[17])
                            self._set_df_column_value_at_index(
                                n, "PutVegaChange", +t[18] - self._get_df_column_value_at_index(n, "PutVega"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutVega", +t[18])
                            self._set_df_column_value_at_index(
                                n, "PutGammaChange", +t[19] - self._get_df_column_value_at_index(n, "PutGamma"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutGamma", +t[19])
                            self._set_df_column_value_at_index(
                                n, "PutIV", 100 * +t[20])
                            self._set_df_column_value_at_index(
                                n, "PutIVChange", -1 * (self._get_df_column_value_at_index(n, "PutPrevIV") - +(100 * +t[20])))  # noqa: E501
                            self._set_df_column_value_at_index(n, "PutIVChange%", (+(100 * +t[20]) - self._get_df_column_value_at_index(  # noqa: E501
                                n, "PutPrevIV")) / self._get_df_column_value_at_index(n, "PutPrevIV"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutPrevIV", 100 * +t[20])
                            self._set_df_column_value_at_index(
                                n, "PutVannaChange", +t[21] - self._get_df_column_value_at_index(n, "PutVanna"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutVanna", +t[21])
                            self._set_df_column_value_at_index(
                                n, "PutCharmChange", +t[22] - self._get_df_column_value_at_index(n, "PutCharm"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutCharm", +t[22])
                            self._set_df_column_value_at_index(
                                n, "PutSpeedChange", +t[23] - self._get_df_column_value_at_index(n, "PutSpeed"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutSpeed", +t[23])
                            self._set_df_column_value_at_index(
                                n, "PutZommaChange", +t[24] - self._get_df_column_value_at_index(n, "PutZomma"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutZomma", +t[24])
                            self._set_df_column_value_at_index(
                                n, "PutColorChange", +t[25] - self._get_df_column_value_at_index(n, "PutColor"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutColor", +t[25])
                            self._set_df_column_value_at_index(
                                n, "PutVolgaChange", +t[26] - self._get_df_column_value_at_index(n, "PutVolga"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutVolga", +t[26])
                            self._set_df_column_value_at_index(
                                n, "PutVetaChange", +t[27] - self._get_df_column_value_at_index(n, "PutVeta"))  # noqa: E501
                            self._set_df_column_value_at_index(
                                n, "PutVeta", +t[27])
                        self._update_option_chain_in_excel_wb(_streaming=True)
                elif "o" == message_type:
                    j = self._get_length(_byte_format=["chh"])
                    print("-- stream connection open --")
                    return self._unpack(_bin, _end=j, _byte_format="chh")
                elif "e" == message_type:
                    j = self._get_length(_byte_format=["chhccc"])
                    print("-- stream connection open --")
                    t = self._unpack(_bin, _end=j, _byte_format="chhccc")
                    print(t.decode('utf-8'))
                    if 1003 == t[2]:
                        print(
                            "Data failed due to max number of instruments exceeded", "Error")  # noqa: E501
                        self.close()
                    if 1004 == t[2]:
                        print(
                            "----- log out ---", "Session expired", "Signin Again")  # noqa: E501
                    if 1005 == t[2]:
                        print("-- connection open another place ----",
                              "Connection already open ")
                        self.close()
            else:
                print("Unable to decode packet, packet length is less than 2")
        except Exception as e:
            print(e.message, e.args)

    def _subscribe_packets_formated(self, _symbol: str, _expiry: str):
        _instrument = f"{_symbol.upper()}:{_expiry}"
        i = len(_instrument)
        byte_format = ["chccc", i, "s"]
        _bin = ["s", 3 + i, "c", "g", "n", _instrument]
        return self._pack(_bin, byte_format="".join(list(map(str, byte_format))))  # noqa: E501

    def _unsubscribe_packets_formated(self, _symbol: str, _expiry: str):
        _instrument = f"{_symbol.upper()}:{_expiry}"
        e = len(_instrument)
        byte_format = ["chc", e, "s"]
        _bin = ["u", 1 + e, "c", _instrument]
        return self._pack(_bin, byte_format="".join(list(map(str, byte_format))))  # noqa: E501


def _validate_sessions(_api_key, _access_token, _app_version):
    try:
        with closing(create_connection(f"wss://wsoc.quantsapp.com/?user_id={_api_key}&token={_access_token}&portal=web&version={_app_version}&country=in",
                                       origin="https://web.quantsapp.com",
                                       host="wsoc.quantsapp.com",
                                       header={'User-Agent': requests.get(
                "https://techfanetechnologies.github.io/latest-user-agent/user_agents.json").json()[-2]})) as ws_user:
            print("Session Validated")
            return True
    except _exceptions.WebSocketBadStatusException:
        print("Session Expired, Logging in again.....")
        return False


def _is_time_between(begin_time, end_time, check_time=None):
    from datetime import datetime, time
    # If check time is not given, default to current UTC time
    check_time = check_time or datetime.utcnow().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time
    else:  # crosses midnight
        return check_time >= begin_time or check_time <= end_time


def _isNowInTimePeriod(startTime, endTime, nowTime):
    if startTime < endTime:
        return nowTime >= startTime and nowTime <= endTime
    else:
        # Over midnight:
        return nowTime >= startTime or nowTime <= endTime
