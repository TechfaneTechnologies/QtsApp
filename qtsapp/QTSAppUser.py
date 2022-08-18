# -*- coding: utf-8 -*-

"""
Created on Mon August 8, 08:09:56 2022
@author: DrJuneMoone
"""

from qtsapp.lib import *
from qtsapp.QTSAppClientFactory import *


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
    _ws_routes = {
        "login": "https://ws.quantsapp.com",
        "user": "https://wsoc.quantsapp.com",
        "user_route": "/",
        "stream": "https://server.quantsapp.com",
        "stream_route": "/stream",
    }
    ws_endpoints = {"login": "wss://ws.quantsapp.com"}
    _cores = os.cpu_count()

    def __init__(
        self,
        user_name: str = None,
        password: str = None,
        api_key: str = None,
        access_token: str = None,
        wb_name: str = None,
        out_queue=None,
        debug: bool = False,
        reconnect: bool = True,
        reconnect_max_tries=RECONNECT_MAX_TRIES,
        reconnect_max_delay=RECONNECT_MAX_DELAY,
        connect_timeout=CONNECT_TIMEOUT,
    ):
        threading.Thread.__init__(self)
        # Set max reconnect tries
        if reconnect_max_tries > self._maximum_reconnect_max_tries:
            log.warning(
                "`reconnect_max_tries` can not be more than {val}. Setting to highest possible value - {val}.".format(
                    val=self._maximum_reconnect_max_tries
                )
            )
            self.reconnect_max_tries = self._maximum_reconnect_max_tries
        else:
            self.reconnect_max_tries = reconnect_max_tries
        # Set max reconnect delay
        if reconnect_max_delay < self._minimum_reconnect_max_delay:
            log.warning(
                "`reconnect_max_delay` can not be less than {val}. Setting to lowest possible value - {val}.".format(
                    val=self._minimum_reconnect_max_delay
                )
            )
            self.reconnect_max_delay = self._minimum_reconnect_max_delay
        else:
            self.reconnect_max_delay = reconnect_max_delay
        self.connect_timeout = connect_timeout
        # Debug enables logs
        self.debug = debug
        self.testing = False
        # Placeholders for callbacks.
        self.on_ticks = None
        self.on_open = None
        self.on_close = None
        self.on_error = None
        self.on_connect = None
        self.on_message = None
        self.on_reconnect = None
        self.on_noreconnect = None
        self._first_run = True
        self._resubscribe = False
        self.start_refreshing = False
        self._wb_name = (
            self._check_n_return_file_path(f"{wb_name}.xlsm")
            if wb_name != "OptionChain"
            else self._check_n_return_file_path("OptionChain.xlsm")
        )
        self._wb = xw.Book(self._wb_name)
        self._ws = self._wb.sheets("RawData")
        # List of current subscribed tokens
        self.subscribed_tokens = []
        self._out_queue = out_queue
        self._col = None
        self._pd = None
        self._df = None
        self._np = None
        self._snp = None
        self._shm = None
        self._user_agent = requests.get(
            "https://techfanetechnologies.github.io/latest-user-agent/user_agents.json"
        ).json()[-2]
        self._get_app_version()
        self._fetch_master_script()
        self._get_instrument_records()
        self._user_config = dotenv_values(".env.secret")
        self._user_name = (
            self._user_config["USER_NAME"]
            if self._user_config
            and len(self._user_config.keys()) != 0
            and "USER_NAME" in self._user_config.keys()
            and user_name is None
            else user_name
        )
        self._password = (
            self._user_config["PASSWORD"]
            if self._user_config
            and len(self._user_config.keys()) != 0
            and "PASSWORD" in self._user_config.keys()
            and password is None
            else password
        )
        self._api_config = dotenv_values(".env")
        self._api_key = (
            self._api_config["API_KEY"]
            if self._api_config
            and len(self._api_config.keys()) != 0
            and "API_KEY" in self._api_config.keys()
            and api_key is None
            else api_key
        )
        self._access_token = (
            self._api_config["ACCESS_TOKEN"]
            if self._api_config
            and len(self._api_config.keys()) != 0
            and "ACCESS_TOKEN" in self._api_config.keys()
            and access_token is None
            else access_token
        )
        self._validate_session()
        self.ws_endpoints["user"] = self._get_url(
            route="user_route",
            params={
                "user_id": self._api_key,
                "token": self._access_token,
                "portal": "web",
                "version": self._app_version,
                "country": "in",
            },
        )
        self.ws_endpoints["streaming"] = self._get_url(
            route="stream_route",
            params={
                "user_id": self._api_key,
                "token": self._access_token,
                "portal": "web",
                "version": self._app_version,
                "country": "in",
                "force_login": "false",
            },
        )
        self.socket_url = self.ws_endpoints["user"]
        self._initiate_queues_and_threads()
        self._lock = threading.Lock()
        self._connected_event = threading.Event()

    def _initiate_queues_and_threads(self):
        self._stream = self._isMarketTime()
        pythoncom.CoInitialize()
        self._ocq = Queue()
        threading.Thread(
            target=self._populate_oc_table_data,
            name=f"Populate OC Table Thread",
            daemon=True,
        ).start()
        threading.Thread(
            target=self._resubscribe_on_instrument_change,
            name=f"Resubscribe On Instrument Change Thread",
            daemon=True,
        ).start()

    def _create_np(self):
        self._np = self._df.to_records(
            index=True,
            column_dtypes={
                "CallDataUpdateTimeStamp": f"<U{self._df.CallDataUpdateTimeStamp.str.len().max()}",
                "PutDataUpdateTimeStamp": f"<U{self._df.PutDataUpdateTimeStamp.str.len().max()}",
                "FutureOrFairPrice": f"<U{self._df.FutureOrFairPrice.str.len().max()}",
                "FutureDataUpdateTimeStamp": f"<U{self._df.FutureDataUpdateTimeStamp.str.len().max()}",
                "CLtpColor": f"<U{self._df.CLtpColor.str.len().max()}",
                "PLtpColor": f"<U{self._df.PLtpColor.str.len().max()}",
                "FLtpColor": f"<U{self._df.FLtpColor.str.len().max()}",
                "Symbol": f"<U{self._df.Symbol.str.len().max()}",
                "Expiry": f"<U{self._df.Expiry.str.len().max()}",
            },
        )
        if (
            not self._first_run
            and not self._resubscribe
            and self._shm is not None
            and self._snp is not None
        ):
            np.copyto(self._snp, self._np)

    def _create_shared_memory(self):
        if (not self._first_run or self._resubscribe) and (
            self._shm is not None and self._snp is not None
        ):
            print("Sending Close SharedMemory Command To The Worker Processes")
            self._out_queue.put_nowait(("shm_close", None, None, None))
            sleep(2)
            if self._out_queue.get() == "Closed_SharedMemory":
                self._shm.close()
                self._shm.unlink()
                del self._shm
                del self._snp
        if self._np is not None:
            self._shm = SharedMemory(
                create=True, name="qts_appuser", size=self._np.nbytes
            )
            self._snp = np.recarray(
                shape=self._np.shape, dtype=self._np.dtype, buf=self._shm.buf
            )
            np.copyto(self._snp, self._np)
            self._out_queue.put_nowait(
                ("shm_open", self._shm.name, self._np.shape, self._np.dtype)
            )

    def _get_shared_mem_args(self):
        if self._np is not None and self._shm is not None:
            self._out_queue.put_nowait(
                ("shm_open", self._shm.name, self._np.shape, self._np.dtype)
            )
            return (self._shm.name, self._np.shape, self._np.dtype)

    def _get_kwargs_dict(self, **kwargs):
        return kwargs

    def _get_args_list(self, *args):
        return args

    def _get_app_version(self):
        _BASE_URL = "https://web.quantsapp.com"
        s = requests.get(_BASE_URL, headers={"User-Agent": self._user_agent})
        mainjs = re.findall(r"main-es2015\.\w+\.js", s.text)
        mainjs = re.findall(r"main.*\w+\.js", s.text)[0].split(" ")[0].replace('"', "")
        mainjs = requests.get(
            f"{_BASE_URL}/{mainjs}", headers={"User-Agent": self._user_agent}
        )
        kiqv = json.loads(
            re.findall(
                r"kiQV\:function\(t\)\{t\.exports\=JSON\.parse\(\'..*\'\)\}\,kmnG",
                mainjs.text,
            )[0].split("'")[1]
        )
        self._app_name, self._app_version, self._app_key = (
            kiqv["name"],
            kiqv["version"],
            kiqv["key"],
        )

    def _auth(self):
        print(
            self._app_version,
            self._user_name,
            self._password,
            self.ws_endpoints["login"].lstrip("wss://"),
            self._user_agent,
        )
        with closing(
            create_connection(
                self.ws_endpoints["login"],
                origin="https://web.quantsapp.com",
                host="ws.quantsapp.com",
                header={"User-Agent": self._user_agent},
            )
        ) as self._ws_auth:
            self._ws_auth.send(
                json.dumps(
                    {
                        "noti_token": "0",
                        "action": "signin",
                        "mode": "login_custom",
                        "platform": "web",
                        "version": self._app_version,  # "2.3.57",
                        "country": "in",
                        "email": self._user_name,
                        "user_password": self._password,
                        "sub_platform": "live",
                        "source": "qapp",
                    }
                )
            )
            msg = json.loads(self._ws_auth.recv())
            # print(msg)
            if (
                msg["status"] != "1"
                and msg["msg"] != "Login Successful"
                and msg["routeKey"] != "signin"
                and msg["custom_key"] != msg["routeKey"]
            ):
                if (
                    msg["msg"]
                    == "Account is mapped with Google login, use the Google button to proceed."
                ):
                    print(
                        """Since Your Account is mapped with Google login,
                        use the Google button to proceed login to QtsApp
                        Website, Then Follow the readme.md guidlines to
                        obtain API_KEY and ACCESS_TOKEN Manually, or
                        Watch Video Tutorial at
                        https://youtu.be/UQ2bM7ileRA

                        """
                    )
                    while (
                        input(
                            "Have You Obtained API_KEY and ACCESS_TOKEN ? Type [Yes/No] >>> "
                        )
                        .strip()
                        .lower()
                        != "yes"
                    ):
                        print(
                            "Waiting for 5 seconds, While you to obtain the API_KEY and ACCESS_TOKEN.\n"
                        )
                        sleep(5)
                    self._api_key = input("Please Input The API_KEY: ").strip()
                    self._access_token = input(
                        "Please Input The ACCESS_TOKEN: "
                    ).strip()
                    with open(".env", "w") as f:
                        f.write(f"API_KEY={self._api_key}\n")
                        f.write(f"ACCESS_TOKEN={self._access_token}\n")
                    self._validate_session()
                else:
                    raise ValueError(f'Failed To Authenticate, With Error {msg["msg"]}')
            if (
                msg["status"] == "1"
                and msg["msg"] == "Login Successful"
                and msg["routeKey"] == "signin"
                and msg["custom_key"] == msg["routeKey"]
                and msg["api_key"]
                and msg["token"]
            ):
                with open(".env", "w") as f:
                    f.write(f"API_KEY={msg['api_key']}\n")
                    f.write(f"ACCESS_TOKEN={msg['token']}\n")
                self._api_key, self._access_token = msg["api_key"], msg["token"]

    def _validate_session(self):
        _validation_successful, _validation_msg = validate_sessions(
            self._api_key, self._access_token, self._app_version
        )
        if (not _validation_successful) and (
            _validation_msg != "Session Validated"
            and _validation_msg == "Session Expired"
        ):
            self._auth()

    def _logout(self):
        try:
            self.ws.sendMessage(
                six.b(
                    json.dumps(
                        {
                            "mode": "logout",
                            "custom_key": "logout",
                            "action": "user_profile",
                            "country": "in",
                            "version": self._app_version,  # "2.3.57",
                            "platform": "web",
                            "sub_platform": "live",
                        }
                    )
                )
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
            "Origin": "https://web.quantsapp.com",
            "Host": "wsoc.quantsapp.com"
            if self.socket_url == self.ws_endpoints["user"]
            else "server.quantsapp.com",
        }

        # Init WebSocket client factory
        self._create_connection(
            self.socket_url, useragent=self._user_agent, proxy=proxy, headers=headers
        )

        # Set SSL context
        context_factory = None
        if self.factory.isSecure and not disable_ssl_verification:
            context_factory = ssl.ClientContextFactory()

        # Establish WebSocket connection to a server
        connectWS(
            self.factory, contextFactory=context_factory, timeout=self.connect_timeout
        )

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
                    target=reactor.run, kwargs=opts
                )
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
                json.dumps(
                    {
                        "Scrip": instrument,  # "NIFTY",
                        "Expiry": expiry,  # "07-Jul-22",
                        "custom_key": "chain",
                        "action": "chain-pain-skew-pcr",
                        "platform": "web",
                        "version": self._app_version,  # "2.3.57",
                        "sub_platform": "live",
                    }
                )
            )
        )
        if 0 < len(self.subscribed_tokens) <= 1:
            self.subscribed_tokens[0] = [instrument, expiry]
        else:
            self.subscribed_tokens.append([instrument, expiry])
        return True

    def resubscribe(self):
        """Resubscribe to all current subscribed tokens."""
        instrument, expiry = self.subscribed_tokens[0][0], self.subscribed_tokens[0][-1]
        if self.debug:
            log.debug("Resubscribe: {} - {}".format(instrument, expiry))
        self.subscribe(instrument, expiry)

    def _resubscribe_on_instrument_change(self):
        pythoncom.CoInitialize()
        while True:
            try:
                instrument, expiry = self._get_instrument_expiry()
                if isinstance(expiry, dtdt):
                    expiry = expiry.strftime("%d-%b-%y")
                print(self.subscribed_tokens[0], [instrument, expiry])
                if self.subscribed_tokens[0] != [instrument, expiry]:
                    if self.debug:
                        log.debug("Resubscribe: {} - {}".format(instrument, expiry))
                    print("Resubscribe: {} - {}".format(instrument, expiry))
                    self._resubscribe = True
                    self.start_refreshing = False
                    self.subscribe(instrument, expiry)
                else:
                    if self._stream and self.start_refreshing:
                        print("Refreshing OptionChain")
                        self.resubscribe()
                        self._stream = self._isMarketTime()
                if self._isMarketTime():
                    sleep(random.uniform(4.93, 5.57))
                else:
                    sleep(1)
            except:
                continue

    def resubscribe_on_instrument_change(self):
        reactor.callFromThread(self._resubscribe_on_instrument_change)

    def _on_connect(self, ws, response):
        self.ws = ws
        if self.on_connect:
            self.on_connect(self, response)
        self._on_init_get_option_chain()

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
            if data["status"] != "1":
                raise ValueError(f"Request UnSuccessfull with msg : {data}")
            elif (
                data["custom_key"] == "chain"
                and data["routeKey"] == "chain-pain-skew-pcr"
            ):
                # print(data)
                self._queue_populate_oc_table_data(_data=data)
            elif (
                data["status"] == "1"
                and data["msg"] == "Logged out Successfully."
                and data["routeKey"] == "user_profile"
                and data["custom_key"] == "logout"
            ):
                print("Logged out Successfully.")
                # print(data)
            else:
                print(data)
        except ValueError:
            return

    def _fetch_master_script(self):
        self._master_script = requests.get(
            "https://techfanetechnologies.github.io/QtSAppMasterScript/masterScript.json"
        ).json()  # noqa: E501
        self._instruments = self._master_script.keys()

    def _get_instrument_records(self):
        _instrument_records = [["SymbolName", "ExpiryDate", "LotSize", "Strikes"]]
        for _symbol in self._instruments:
            _instrument_records.extend(
                [
                    [
                        _symbol,
                        self._master_script[_symbol]["expiry"][_idx],
                        lot,
                        len(self._master_script[_symbol]["strikes"][0]),
                    ]
                    for _idx, lot in enumerate(self._master_script[_symbol]["lot"])
                ]
            )
        self._instrument_records = pd.DataFrame(_instrument_records)

    def _get_lot_value(self, _symbol: str, _expiry: str):
        if (
            _symbol.upper() in self._instruments
            and _expiry in self._master_script[_symbol]["expiry"]
        ):  # noqa: E501
            return self._master_script[_symbol]["lot"][
                self._master_script[_symbol]["expiry"].index(_expiry)
            ]  # noqa: E501
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_expiry_dates(self, _symbol: str):
        if _symbol.upper() in self._instruments:
            return self._master_script[_symbol]["expiry"]
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_list_of_strikes(self, _symbol: str, _expiry: str):
        if (
            _symbol.upper() in self._instruments
            and _expiry in self._master_script[_symbol]["expiry"]
        ):  # noqa: E501
            return self._master_script[_symbol]["strikes"][
                self._master_script[_symbol]["expiry"].index(_expiry)
            ]  # noqa: E501
        else:
            print(f"{_symbol} not found !")
            return None

    def _get_atm_strike(self, _symbol: str, _expiry: str, _ltp: float):
        all_strikes = self._get_list_of_strikes(_symbol, _expiry)
        atm_strike = int(min(all_strikes, key=lambda x: abs(x - _ltp)))
        return atm_strike

    def _find_atm_strike(self, _all_strikes: pd.core.series.Series, _ltp: float):
        return int(min(_all_strikes.astype(int).tolist(), key=lambda x: abs(x - _ltp)))

    def _set_df_column_value_at_index(
        self, _df: pd.core.frame.DataFrame, _index: int, _column_name: str, _value
    ):
        _df.at[_index, _column_name] = _value

    def _get_df_column_value_at_index(
        self, _df: pd.core.frame.DataFrame, _index: int, _column_name: str
    ):
        return _df.at[_index, _column_name]

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

    def _isMarketTime(self):
        if self.testing:
            return True
        else:
            return self._isNowInTimePeriod(
                dt.time(9, 15), dt.time(15, 30), dtdt.now().time()
            ) and (dtdt.now().strftime("%A") not in ["Saturday", "Sunday"])

    def _queue_populate_oc_table_data(self, _data: typing.Dict):
        self._ocq.put_nowait(_data)
        self._ocq.join()

    def _populate_oc_table_data(self):
        def tprint(*args, **kwargs):
            with self._lock:
                print(*args, **kwargs)

        pythoncom.CoInitialize()
        while True:
            try:
                _response = self._ocq.get_nowait()
                # print(_response)
            except queue.Empty:
                _response = None
                continue
            try:
                if _response is not None:
                    if (
                        _response["status"] == "1"
                        and _response["msg"] == "success"
                        and _response["statusCode"] == 200
                    ):
                        timestamp = _response["timestamp"]
                        try:
                            strike_list = list(map(int, _response["strike"].split(",")))
                        except ValueError:
                            # print(ValueError)
                            strike_list = list(
                                map(float, _response["strike"].split(","))
                            )
                        strike_list_length_int_array = [int(0)] * len(strike_list)
                        strike_list_length_str_array = [str(0)] * len(strike_list)
                        strike_list_length_float_array = [float(0)] * len(strike_list)
                        futureprice = float(_response["rp"].split(",")[0])
                        futureorfairprice = (
                            "FuturePrice"
                            if _response["rp"].split(",")[1] == "0"
                            else "FairPrice"
                            if _response["rp"].split(",")[1] == "1"
                            else "Unknown"
                        )
                        futuredataupdatetimestamp = dtdt.strptime(
                            timestamp, "%d-%b-%y %H:%M:%S"
                        )
                        oc_table = {
                            "PutClose": strike_list_length_float_array,
                            "PutLow": strike_list_length_float_array,
                            "PutHigh": strike_list_length_float_array,
                            "PutOpen": strike_list_length_float_array,
                            "PutVetaChange": strike_list_length_float_array,
                            "PutVeta": strike_list_length_float_array,
                            "PutVolgaChange": strike_list_length_float_array,
                            "PutVolga": strike_list_length_float_array,
                            "PutColorChange": strike_list_length_float_array,
                            "PutColor": strike_list_length_float_array,
                            "PutZommaChange": strike_list_length_float_array,
                            "PutZomma": strike_list_length_float_array,
                            "PutSpeedChange": strike_list_length_float_array,
                            "PutSpeed": strike_list_length_float_array,
                            "PutCharmChange": strike_list_length_float_array,
                            "PutCharm": strike_list_length_float_array,
                            "PutVannaChange": strike_list_length_float_array,
                            "PutVanna": strike_list_length_float_array,
                            "PutGammaChange": strike_list_length_float_array,
                            "PutGamma": list(
                                map(float, _response["p_gamma"].split(","))
                            ),
                            "PutVegaChange": strike_list_length_float_array,
                            "PutVega": list(map(float, _response["p_vega"].split(","))),
                            "PutThetaChange": strike_list_length_float_array,
                            "PutTheta": list(
                                map(float, _response["p_theta"].split(","))
                            ),
                            "PutDeltaChange": strike_list_length_float_array,
                            "PutDelta": list(
                                map(float, _response["p_delta"].split(","))
                            ),
                            "PutPrevIV": list(
                                map(float, _response["p_prev_iv"].split(","))
                            ),
                            "PutVolume": list(
                                map(int, _response["p_volume"].split(","))
                            ),
                            "PutOIChange": list(
                                map(int, _response["p_oi_change"].split(","))
                            ),
                            "PutOI": list(map(int, _response["p_oi"].split(","))),
                            "PutIV": list(map(float, _response["p_iv"].split(","))),
                            "PutLtpChange": list(
                                map(float, _response["p_ltp_change"].split(","))
                            ),
                            "PutLtp": list(map(float, _response["p_ltp"].split(","))),
                            "CallLtp": list(map(float, _response["c_ltp"].split(","))),
                            "CallLtpChange": list(
                                map(float, _response["c_ltp_change"].split(","))
                            ),
                            "CallIV": list(map(float, _response["c_iv"].split(","))),
                            "CallOI": list(map(int, _response["c_oi"].split(","))),
                            "CallOIChange": list(
                                map(int, _response["c_oi_change"].split(","))
                            ),
                            "CallVolume": list(
                                map(int, _response["c_volume"].split(","))
                            ),
                            "CallDelta": list(
                                map(float, _response["c_delta"].split(","))
                            ),
                            "CallDeltaChange": strike_list_length_float_array,
                            "CallTheta": list(
                                map(float, _response["c_theta"].split(","))
                            ),
                            "CallThetaChange": strike_list_length_float_array,
                            "CallVega": list(
                                map(float, _response["c_vega"].split(","))
                            ),
                            "CallVegaChange": strike_list_length_float_array,
                            "CallGamma": list(
                                map(float, _response["c_gamma"].split(","))
                            ),
                            "CallGammaChange": strike_list_length_float_array,
                            "CallPrevIV": list(
                                map(float, _response["c_prev_iv"].split(","))
                            ),
                            "CallVanna": strike_list_length_float_array,
                            "CallVannaChange": strike_list_length_float_array,
                            "CallCharm": strike_list_length_float_array,
                            "CallCharmChange": strike_list_length_float_array,
                            "CallSpeed": strike_list_length_float_array,
                            "CallSpeedChange": strike_list_length_float_array,
                            "CallZomma": strike_list_length_float_array,
                            "CallZommaChange": strike_list_length_float_array,
                            "CallColor": strike_list_length_float_array,
                            "CallColorChange": strike_list_length_float_array,
                            "CallVolga": strike_list_length_float_array,
                            "CallVolgaChange": strike_list_length_float_array,
                            "CallVeta": strike_list_length_float_array,
                            "CallVetaChange": strike_list_length_float_array,
                            "CallOpen": strike_list_length_float_array,
                            "CallHigh": strike_list_length_float_array,
                            "CallLow": strike_list_length_float_array,
                            "CallClose": strike_list_length_float_array,
                            "CallAskPrice": strike_list_length_float_array,
                            "CallAskQty": strike_list_length_int_array,
                            "CallBidPrice": strike_list_length_float_array,
                            "CallBidQty": strike_list_length_int_array,
                            "CallAveragePrice": strike_list_length_float_array,
                            "PutAskPrice": strike_list_length_float_array,
                            "PutAskQty": strike_list_length_int_array,
                            "PutBidPrice": strike_list_length_float_array,
                            "PutBidQty": strike_list_length_int_array,
                            "PutAveragePrice": strike_list_length_float_array,
                            "CallDataUpdateTimeStamp": list(
                                map(
                                    lambda x: timestamp,
                                    strike_list_length_int_array,
                                )
                            ),
                            "PutDataUpdateTimeStamp": list(
                                map(
                                    lambda x: timestamp,
                                    strike_list_length_int_array,
                                )
                            ),
                            "FuturePrice": list(
                                map(
                                    lambda x: futureprice,
                                    strike_list_length_float_array,
                                )
                            ),
                            "FutureOrFairPrice": list(
                                map(
                                    lambda x: futureorfairprice,
                                    strike_list_length_str_array,
                                )
                            ),
                            "FutureDataUpdateTimeStamp": list(
                                map(
                                    lambda x: timestamp,
                                    strike_list_length_int_array,
                                )
                            ),
                        }

                        _df = pd.DataFrame(
                            oc_table, index=strike_list, columns=oc_table.keys()
                        )
                        self._atm_strike = (
                            _response["astrike"]
                            if self._find_atm_strike(_df.index, futureprice)
                            == _response["astrike"]
                            else self._find_atm_strike(_df.index, futureprice)
                        )
                        _df.eval("PutIVChange = PutIV - PutPrevIV", inplace=True)
                        _df.eval(
                            "PutIVChangePercent = PutIVChange / PutPrevIV", inplace=True
                        )
                        _df.eval("PutPrevOI = PutOI - PutOIChange", inplace=True)
                        _df.eval(
                            "PutOIChangePercent = PutOIChange / PutPrevOI", inplace=True
                        )
                        _df.eval("PutPrevLTP = PutLtp - PutLtpChange", inplace=True)
                        _df.eval(
                            "PutLtpChangePercent = PutLtpChange / PutPrevLTP",
                            inplace=True,
                        )
                        _df.eval("CallPrevLTP = CallLtp - CallLtpChange", inplace=True)
                        _df.eval(
                            "CallLtpChangePercent = CallLtpChange / CallPrevLTP",
                            inplace=True,
                        )
                        _df.eval("CallPrevOI = CallOI - CallOIChange", inplace=True)
                        _df.eval(
                            "CallOIChangePercent = CallOIChange / CallPrevOI",
                            inplace=True,
                        )
                        _df.eval("CallIVChange = CallIV - CallPrevIV", inplace=True)
                        _df.eval(
                            "CallIVChangePercent = CallIVChange / CallPrevIV",
                            inplace=True,
                        )
                        _df.eval("immediatePrevCLtp = CallPrevLTP", inplace=True)
                        _df.eval("immediatePrevPLtp = PutPrevLTP", inplace=True)
                        _df.eval("immediatePrevFLtp = FuturePrice", inplace=True)
                        _df.insert(
                            96,
                            "CLtpColor",
                            np.select(
                                (
                                    (_df["CallPrevLTP"] - _df["CallLtp"]) > 0,
                                    (_df["CallPrevLTP"] - _df["CallLtp"]) < 0,
                                ),
                                ("R", "G"),
                                "W",
                            ),
                        )
                        _df.insert(
                            97,
                            "PLtpColor",
                            np.select(
                                (
                                    (_df["PutPrevLTP"] - _df["PutLtp"]) > 0,
                                    (_df["PutPrevLTP"] - _df["PutLtp"]) < 0,
                                ),
                                ("R", "G"),
                                "W",
                            ),
                        )
                        _df.insert(
                            98,
                            "FLtpColor",
                            np.select(
                                (
                                    (_df["immediatePrevFLtp"] - _df["FuturePrice"]) > 0,
                                    (_df["immediatePrevFLtp"] - _df["FuturePrice"]) < 0,
                                ),
                                ("R", "G"),
                                "W",
                            ),
                        )
                        self._maxcalloi, self._maxcalloi_strike = (
                            _df["CallOI"].max(),
                            _df["CallOI"].idxmax(),
                        )
                        self._mincalloi, self._mincalloi_strike = (
                            _df["CallOI"].min(),
                            _df["CallOI"].idxmin(),
                        )
                        self._maxputoi, self._maxputoi_strike = (
                            _df["PutOI"].max(),
                            _df["PutOI"].idxmax(),
                        )
                        self._minputoi, self._minputoi_strike = (
                            _df["PutOI"].min(),
                            _df["PutOI"].idxmin(),
                        )
                        _df["ATMStrike"] = self._atm_strike
                        _df["MaxCallOI"] = self._maxcalloi
                        _df["MaxCallOIStrike"] = self._maxcalloi_strike
                        _df["MinCallOI"] = self._mincalloi
                        _df["MinCallOIStrike"] = self._mincalloi_strike
                        _df["MaxPutOI"] = self._maxputoi
                        _df["MaxPutOIStrike"] = self._maxputoi_strike
                        _df["MinPutOI"] = self._minputoi
                        _df["MinPutOIStrike"] = self._minputoi_strike
                        _df["Symbol"] = self.subscribed_tokens[0][0]
                        _df["Expiry"] = (
                            dtdt.strptime(
                                self.subscribed_tokens[0][-1], "%d-%b-%y"
                            ).strftime("%d-%b-%y %H:%M:%S")
                            if isinstance(self.subscribed_tokens[0][-1], str)
                            else self.subscribed_tokens[0][-1].strftime(
                                "%d-%b-%y %H:%M:%S"
                            )
                        )
                        _pd = {"StrikePrice": strike_list} | _df.to_dict()
                        # print(_df)
                        # pythoncom.CoInitialize()
                        self._df, self._pd, self._col = (
                            _df,
                            _pd,
                            {k: v for v, k in enumerate(list(_pd.keys()))},
                        )
                        self._create_np()
                        if self._first_run or self._resubscribe:
                            self._create_shared_memory()
                            self._clear_oc_table_data()
                            self._first_run = False
                            self._resubscribe = False
                        self.start_refreshing = True
                    self._ocq.task_done()
            except Exception as e:
                print(f"An Exception {e} has occured in populate oc table thread....")
                continue

    def _check_n_return_file_path(self, _file_path):
        try:
            if (
                os.path.exists(os.path.join(os.getcwd(), _file_path))
                and os.path.isfile(os.path.join(os.getcwd(), _file_path))
            ) and not os.path.isdir(os.path.join(os.getcwd(), _file_path)):
                _file_path = os.path.join(os.getcwd(), _file_path)
                return _file_path
            else:
                print(
                    f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again"
                )  # noqa: E501
                raise ValueError(
                    f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again"
                )  # noqa: E501
        except ValueError:
            raise ValueError(
                f"The Specified File {_file_path} Does Not Exist, Please Create The File & Try Again"
            )  # noqa: E501

    def _clear_oc_table_data(self):
        pythoncom.CoInitialize()
        self._clear_val("O307:DJ607")

    def _get_instrument_expiry(self):
        pythoncom.CoInitialize()
        _symbol, _expiry = self._get_val("E3"), self._get_val("F3")
        return _symbol, _expiry

    def _update_instrument_records(self):
        pythoncom.CoInitialize()
        self._update_val(
            "A1",
            self._instrument_records,
            convert=pd.DataFrame,
            index=False,
            header=False,
            expand="table",
        )

    def _on_init_get_option_chain(self, _symbol: str = None, _expiry: str = None):
        pythoncom.CoInitialize()
        self._update_instrument_records()
        sleep(3)
        while True:
            try:
                if not _symbol and not _expiry:
                    _symbol, _expiry = self._get_instrument_expiry()
                _instrument = f'{_symbol}:{_expiry.strftime("%d%m%Y")}'
                print(f"{_instrument}")
                break
            except AttributeError:
                continue
        self.subscribe(_symbol, _expiry.strftime("%d-%b-%y"))

    def _get_url(self, route: str, params: typing.Dict[str, str] = {}):
        raw_url = PreparedRequest()
        if params and "user" in route:
            raw_url.prepare_url(
                f"{self._ws_routes['user']}{self._ws_routes[route]}", params
            )
            return raw_url.url.replace("https", "wss")
        if params and "stream" in route:
            raw_url.prepare_url(
                f"{self._ws_routes['stream']}{self._ws_routes[route]}", params
            )
            return raw_url.url.replace("https", "wss")

    def _update_formula(self, _method: str, _ws_range: str, _formula, **_options):
        pythoncom.CoInitialize()
        _ws = xw.Book(self._wb_name).sheets("RawData")
        if _method == "formula" and _options:
            _ws.range(_ws_range).options(**_options).formula = _formula
        elif _method == "formula" and not _options:
            _ws.range(_ws_range).formula = _formula
        elif _method == "formula2" and _options:
            _ws.range(_ws_range).options(**_options).formula2 = _formula
        elif _method == "formula2" and not _options:
            _ws.range(_ws_range).formula2 = _formula
        elif _method == "formula_array" and _options:
            _ws.range(_ws_range).options(**_options).formula_array = _formula
        elif _method == "formula_array" and not _options:
            _ws.range(_ws_range).formula_array = _formula
        else:
            _ws.range(_ws_range).formula = _formula

    def _update_val(self, _ws_range: str, _value, **_options):
        pythoncom.CoInitialize()
        _ws = xw.Book(self._wb_name).sheets("RawData")
        if _options:
            _ws.range(_ws_range).options(**_options).value = _value
        else:
            _ws.range(_ws_range).value = _value

    def _get_val(self, _ws_range: str, **_options):
        pythoncom.CoInitialize()
        _ws = xw.Book(self._wb_name).sheets("RawData")
        if _options:
            return _ws.range(_ws_range).options(**_options).value
        else:
            return _ws.range(_ws_range).value

    def _clear_val(self, _ws_range: str):
        pythoncom.CoInitialize()
        _ws = xw.Book(self._wb_name).sheets("RawData")
        _ws.range(_ws_range).clear_contents()

    def _get_cell_column(self, _ws_range: str):
        _cell_name, _column_range = (
            (f"{_ws_range.split(':')[0]}2", _ws_range)
            if ":" in _ws_range
            else (
                _ws_range,
                f"{''.join([i for i in _ws_range if not i.isdigit()])}:{''.join([i for i in _ws_range if not i.isdigit()])}",
            )
        )
        return _cell_name, _column_range

    def _get_last_empty_row(self, _ws_range: str):
        pythoncom.CoInitialize()
        _ws = xw.Book(self._wb_name).sheets("RawData")
        _cell_name, _column_range = self._get_cell_column(_ws_range)
        _ler = _ws.range(_cell_name).value
        _wb = _ws.range(_column_range)
        _ler = 1 if _ler is None else int((_wb.end("down").address).split("$")[-1])
        return _ler

    def _append_val(
        self,
        _ws_range: str,
        _ler: int,
        _value,
        **_options,
    ):
        pythoncom.CoInitialize()
        _ws = xw.Book(self._wb_name).sheets("RawData")
        if _options:
            _ws.range(_ws_range).current_region.end("up").offset(_ler, 0).options(
                **_options
            ).value = _value
        else:
            _ws.range(_ws_range).current_region.end("up").offset(_ler, 0).value = _value
