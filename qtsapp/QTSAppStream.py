# -*- coding: utf-8 -*-

"""
Created on Mon August 8, 08:09:56 2022
@author: DrJuneMoone
"""

from qtsapp.lib import *
from qtsapp.QTSAppClientFactory import *


class QTSAppStream(threading.Thread):
    FORMAT_CHARACTERS = {
        "?": {
            "C_Type": "_Bool",
            "Format": "?",
            "Python_Type": "bool",
            "Standard_Size": 1,
        },
        "B": {
            "C_Type": "unsigned char",
            "Format": "B",
            "Python_Type": "integer",
            "Standard_Size": 1,
        },
        "H": {
            "C_Type": "unsigned short",
            "Format": "H",
            "Python_Type": "integer",
            "Standard_Size": 2,
        },
        "I": {
            "C_Type": "unsigned int",
            "Format": "I",
            "Python_Type": "integer",
            "Standard_Size": 4,
        },
        "L": {
            "C_Type": "unsigned long",
            "Format": "L",
            "Python_Type": "integer",
            "Standard_Size": 4,
        },
        "N": {
            "C_Type": "size_t",
            "Format": "N",
            "Python_Type": "integer",
            "Standard_Size": 0,
        },
        "Q": {
            "C_Type": "unsigned long long",
            "Format": "Q",
            "Python_Type": "integer",
            "Standard_Size": 8,
        },
        "b": {
            "C_Type": "signed char",
            "Format": "b",
            "Python_Type": "integer",
            "Standard_Size": 1,
        },
        "c": {
            "C_Type": "char",
            "Format": "c",
            "Python_Type": "bytes of length 1",
            "Standard_Size": 1,
        },
        "e": {
            "C_Type": "-6",
            "Format": "e",
            "Python_Type": "float",
            "Standard_Size": 2,
        },
        "f": {
            "C_Type": "float",
            "Format": "f",
            "Python_Type": "float",
            "Standard_Size": 4,
        },
        "h": {
            "C_Type": "short",
            "Format": "h",
            "Python_Type": "integer",
            "Standard_Size": 2,
        },
        "i": {
            "C_Type": "int",
            "Format": "i",
            "Python_Type": "integer",
            "Standard_Size": 4,
        },
        "l": {
            "C_Type": "long",
            "Format": "l",
            "Python_Type": "integer",
            "Standard_Size": 4,
        },
        "n": {
            "C_Type": "ssize_t",
            "Format": "n",
            "Python_Type": "integer",
            "Standard_Size": 0,
        },
        "q": {
            "C_Type": "long long",
            "Format": "q",
            "Python_Type": "integer",
            "Standard_Size": 8,
        },
        "x": {
            "C_Type": "pad byte",
            "Format": "x",
            "Python_Type": "no value",
            "Standard_Size": 0,
        },
    }

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
    ws_endpoints = {"login": "wss://ws.quantsapp.com", "testing": "ws://127.0.0.1:8765"}
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
        self._wb_name = (
            self._check_n_return_file_path(f"{wb_name}.xlsm")
            if wb_name != "OptionChain"
            else self._check_n_return_file_path("OptionChain.xlsm")
        )
        self._wb = xw.Book(self._wb_name)
        self._ws = self._wb.sheets("RawData")
        # List of to be subscribed tokens
        self.to_be_subscribed_tokens = []
        # List of current subscribed tokens
        self.subscribed_tokens = []
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
        self.socket_url = self.ws_endpoints["streaming"]
        if self.testing:
            self.socket_url = self.ws_endpoints["testing"]
        self._subscribed_successful, self._unsubscribed_successful = False, False
        self._out_queue = out_queue
        self._col = None
        self._pd = None
        self._df = None
        self._np = None
        self._snp = None
        self._shm = None
        self._lock = threading.Lock()
        self._initiate_queue_and_threads_and_processes()
        self._connected_event = threading.Event()

    def _initiate_queue_and_threads_and_processes(self):
        self._wsq = Queue()
        self._ocq = Queue()
        self._fpdq = Queue()
        self._ipdq = Queue()
        self._process_at = None
        self._join_at = None
        pythoncom.CoInitialize()
        threading.Thread(
            target=self._populate_oc_table_data,
            name=f"Populate OC Table Thread",
            daemon=True,
        ).start()
        threading.Thread(
            target=self._q_join_thread,
            name="Process Decoded Packets Queue Join Thread",
            daemon=True,
        ).start()
        threading.Thread(
            target=self._intermeditae_q,
            name="Intermediate Process Decoded Packets Queue Join Thread",
            daemon=True,
        ).start()
        threading.Thread(
            target=self._decode_packets,
            name=f"Decode Packets Thread",
            daemon=True,
        ).start()
        for i in range(self._cores):
            threading.Thread(
                target=self._process_decoded_packet,
                name=f"Process Decoded Packets Thread.No.{i}",
                daemon=True,
            ).start()
        if not self.testing:
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
                create=True, name="qts_appstream", size=self._np.nbytes
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

    def _get_str_from_list(self, _list: list, _method: str = "std"):
        string = str(_list)[1:-1]
        if _method == "re":
            string = re.sub(r"'", "", string)
            string = re.sub(r" ", "", string)
            return string
        else:
            return string.replace("'", "").replace(" ", "")

    def _isMarketTime(self):
        if self.testing:
            return True
        else:
            return self._isNowInTimePeriod(
                dt.time(9, 15), dt.time(15, 30), dtdt.now().time()
            ) and (dtdt.now().strftime("%A") not in ["Saturday", "Sunday"])

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
        from contextlib import closing
        from websocket import create_connection

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
        with closing(
            create_connection(
                f"wss://wsoc.quantsapp.com/?user_id={self._api_key}&token={self._access_token}&portal=web&version={self._app_version}&country=in",
                origin="https://web.quantsapp.com",
                host="wsoc.quantsapp.com",
                header={"User-Agent": self._user_agent},
            )
        ) as _ws_user:
            _ws_user.send(
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
            msg = json.loads(_ws_user.recv())
            # {"status": "1", "msg": "Logged out Successfully.", "routeKey": "user_profile", "custom_key": "logout"}
            if (
                msg["status"] != "1"
                and msg["msg"] != "Logged out Successfully"
                and msg["routeKey"] != "user_profile"
                and msg["custom_key"] != "logout"
            ):
                print(f'Failed To Logout, With Error {msg["msg"]}')
                return False
            if (
                msg["status"] == "1"
                and msg["msg"] == "Logged out Successfully"
                and msg["routeKey"] == "user_profile"
                and msg["custom_key"] == "logout"
            ):
                print(f'{msg["msg"]}')
                return True

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
            "Host": "server.quantsapp.com",
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

    def subscribe(self, _payload, _isBinary=True):
        """
        Subscribe to a list of instrument_tokens.

        - `instrument_tokens` is list of instrument instrument_tokens to subscribe
        """
        self.ws.sendMessage(_payload, _isBinary)
        return True

    def _send_stream_subscribe_request(self, instrument, expiry):
        self._get_oc_table_data(_instrument=instrument, _expiry=expiry)
        if isinstance(expiry, dtdt):
            expiry = expiry.strftime("%d%m%Y")
        _instrument = f"{instrument.upper()}:{expiry}"
        if self._isMarketTime():
            print(f"----Sending Subscribe Stream Request for {_instrument}----")
            self.subscribe(self._subscribe_packets_formated(instrument, expiry))
        else:
            if self._isNowInTimePeriod(
                dt.time(0, 0), dt.time(9, 15), dt.datetime.now().time()
            ):
                print(
                    f"Sending Subscribe Stream Request for {_instrument} failed due to streaming attempt pre market hours ---"
                )
            elif self._isNowInTimePeriod(
                dt.time(15, 30), dt.time(23, 59), dt.datetime.now().time()
            ):
                print(
                    f"Sending Subscribe Stream Request for {_instrument} failed due to streaming attempt post market hours  ---"
                )
            else:
                print(f"Sending Subscribe Stream Request for {_instrument} failed !!!")

    def _send_stream_unsubscribe_request(self, instrument, expiry):
        if isinstance(expiry, dtdt):
            expiry = expiry.strftime("%d%m%Y")
        _instrument = f"{instrument.upper()}:{expiry}"
        print(f"---Sending UnSubscribe Stream Request for {_instrument}----")
        self.subscribe(self._unsubscribe_packets_formated(instrument, expiry))
        sleep(10)

    def _resubscribe_on_instrument_change(self):
        pythoncom.CoInitialize()
        while True:
            try:
                instrument, expiry = self._get_instrument_expiry()
                if isinstance(expiry, dtdt):
                    _expiry = expiry.strftime("%d%m%Y")
                _instrument = f"{instrument.upper()}:{_expiry}"
                print(self.subscribed_tokens[0], _instrument)
                try:
                    if self.subscribed_tokens[0] != _instrument:
                        if self.debug:
                            log.debug("Resubscribe: {} - {}".format(instrument, expiry))
                        prev_instrument, prev_expiry = (
                            self.subscribed_tokens[0].split(":")[0],
                            self.subscribed_tokens[0].split(":")[-1],
                        )
                        self._send_stream_unsubscribe_request(
                            prev_instrument, prev_expiry
                        )
                        self.to_be_subscribed_tokens.append([instrument, expiry])
                        self._resubscribe = True
                        self._start_exw = False
                        self._send_stream_subscribe_request(instrument, expiry)
                except IndexError:
                    pass
                if self._isMarketTime():
                    sleep(random.uniform(15.75, 20.25))
            except:
                continue

    def resubscribe_on_instrument_change(self):
        # if self._connected_event.wait(timeout=10):
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

        # If the message is binary, parse it and send it to the callback.
        # if self.on_ticks and is_binary and len(payload) > 4:
        #     self.on_ticks(self, self._parse_binary(payload))

        if is_binary and len(payload) >= 2:
            self._wsq.put_nowait(payload)
            # print(payload)

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
            "https://techfanetechnologies.github.io/QtSAppMasterScript/masterScript.json"
        ).json()  # noqa: E501
        self._instruments = self._master_script.keys()

    def _get_instrument_records(self):
        _instrument_records_cols = ["SymbolName", "ExpiryDate", "LotSize", "Strikes"]
        # print(self._instruments)
        _instrument_records = [
            [
                _symbol,
                self._master_script[_symbol]["expiry"][_idx],
                lot,
                len(self._master_script[_symbol]["strikes"][0]),
            ]
            for _symbol in self._instruments
            for _idx, lot in enumerate(self._master_script[_symbol]["lot"])
        ]
        self._instrument_records = pd.DataFrame(
            _instrument_records, columns=_instrument_records_cols
        )

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

    def _get_oc_table_data(self, _instrument, _expiry):
        if _instrument is not None and _expiry is not None:
            if isinstance(_expiry, dtdt):
                _expiry = _expiry.strftime("%d-%b-%y")
            with closing(
                create_connection(
                    self.ws_endpoints["user"],
                    origin="https://web.quantsapp.com",
                    host="wsoc.quantsapp.com",
                    header={"User-Agent": self._user_agent},
                )
            ) as _ws_user:
                _ws_user.send(
                    json.dumps(
                        {
                            "Scrip": _instrument,  # "NIFTY",
                            "Expiry": _expiry,  # "07-Jul-22",
                            "custom_key": "chain",
                            "action": "chain-pain-skew-pcr",
                            "platform": "web",
                            "version": self._app_version,  # "2.3.57",
                            "sub_platform": "live",
                        }
                    )
                )
                msg = json.loads(_ws_user.recv())
                # print(msg)
                if (
                    msg["status"] != "1"
                    and msg["msg"] != "success"
                    and msg["custom_key"] != "chain"
                    and msg["routeKey"] != "chain-pain-skew-pcr"
                ):
                    raise ValueError(f"failed to fetch oc_table, Error {msg}")
                if (
                    msg["status"] == "1"
                    and msg["msg"] == "success"
                    and msg["statusCode"] == 200
                    and msg["custom_key"] == "chain"
                    and msg["routeKey"] == "chain-pain-skew-pcr"
                ):
                    self._ocq.put_nowait(msg)
                    self._ocq.join()

    def _populate_oc_table_data(self):
        def tprint(*args, **kwargs):
            with self._lock:
                print(*args, **kwargs)

        while True:
            try:
                _response = self._ocq.get_nowait()
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
                        # with pd.option_context('display.max_rows', None,
                        # 'display.max_columns', None):
                        #     print(_df)
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
                        _df["Symbol"] = self.to_be_subscribed_tokens[0][0]
                        _df["Expiry"] = (
                            dtdt.strptime(
                                self.to_be_subscribed_tokens[0][-1], "%d-%b-%y"
                            ).strftime("%d-%b-%y %H:%M:%S")
                            if isinstance(self.to_be_subscribed_tokens[0][-1], str)
                            else self.to_be_subscribed_tokens[0][-1].strftime(
                                "%d-%b-%y %H:%M:%S"
                            )
                        )
                        _pd = {"StrikePrice": strike_list} | _df.to_dict()
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
                    self._ocq.task_done()
            except Exception as e:
                print(f"An Exception {e} has occured in populate oc table thread....")
                continue

    def _q_join_thread(self):
        while True:
            if (
                self._join_at is not None
                and self._fpdq.unfinished_tasks >= self._join_at
            ):
                with self._lock:
                    print(f"Stream Queue Join Thread")
                    print(
                        f"Unfinished Tasks are {self._fpdq.unfinished_tasks} >= {self._join_at}  -->  Joining Now"
                    )
                self._fpdq.join()

    def _intermeditae_q(self):
        _no_process_at = True
        while True:
            if _no_process_at:
                try:
                    self._fpdq.put_nowait(self._ipdq.get_nowait())
                    self._ipdq.task_done()
                except queue.Empty:
                    continue
            else:
                if (
                    self._process_at is not None
                    and self._ipdq.unfinished_tasks >= self._process_at
                ):
                    for i in range(self._process_at):
                        try:
                            self._fpdq.put_nowait(self._ipdq.get_nowait())
                            self._ipdq.task_done()
                        except queue.Empty:
                            continue
                else:
                    try:
                        self._fpdq.put_nowait(self._ipdq.get_nowait())
                        self._ipdq.task_done()
                    except queue.Empty:
                        continue

    def _update_max_min_oi(self, _symbol: str, _expiry):
        try:
            if self._snp is not None:
                self._snp["MaxCallOI"] = self._snp["CallOI"].max()
                self._snp["MaxCallOIStrike"] = self._snp["index"][
                    self._snp["CallOI"].argmax()
                ]
                self._snp["MinCallOI"] = self._snp["CallOI"].min()
                self._snp["MinCallOIStrike"] = self._snp["index"][
                    self._snp["CallOI"].argmin()
                ]
                self._snp["MaxPutOI"] = self._snp["PutOI"].max()
                self._snp["MaxPutOIStrike"] = self._snp["index"][
                    self._snp["PutOI"].argmax()
                ]
                self._snp["MinPutOI"] = self._snp["PutOI"].min()
                self._snp["MinPutOIStrike"] = self._snp["index"][
                    self._snp["PutOI"].argmin()
                ]
                self._snp["Symbol"] = _symbol
                self._snp["Expiry"] = (
                    dtdt.strptime(_expiry, "%d-%b-%Y").strftime("%d-%b-%Y %H:%M:%S")
                    if isinstance(_expiry, str)
                    else _expiry.strftime("%d-%b-%Y %H:%M:%S")
                )
        except Exception as e:
            print(f"An Exception {e} has Occurred in _update_max_min_oi function...")
            # raise e
            pass

    def _process_decoded_packet(self):
        def tprint(*args, **kwargs):
            with self._lock:
                print(*args, **kwargs)

        while True:
            try:
                _type, _decoded = self._fpdq.get_nowait()
                # print(_type, _decoded)
            except queue.Empty:
                _type, _decoded = None, None
                continue
            try:
                if _type is not None and _decoded is not None and self._snp is not None:
                    # with self._lock:
                    if _type == "Future":
                        _symbol, _expiry, _seacsof, _t = _decoded
                        if isinstance(_expiry, str):
                            _expiry = dtdt.strptime(_expiry, "%d%m%Y").strftime(
                                "%d-%b-%Y"
                            )
                        if len(_seacsof) == 2:
                            (
                                FutureDataUpdateTimeStamp,
                                FuturePrice,
                                FLtpColor,
                                immediatePrevFLtp,
                            ) = (
                                self._qts_app_ts_decode(+_t[4], _string=True),
                                +_t[8],
                                (
                                    "R"
                                    if (self._snp["immediatePrevFLtp"][0] - +_t[8]) > 0
                                    else "G"
                                    if (self._snp["immediatePrevFLtp"][0] - +_t[8]) < 0
                                    else "W"
                                ),
                                +_t[8],
                            )
                            _print = json.dumps(
                                {
                                    "Symbol": _symbol,
                                    "Expiry": _expiry,
                                    "InstrumentType": _type,
                                    "DataUpdateTimeStamp": FutureDataUpdateTimeStamp,
                                    "Close/Ltp": FuturePrice,
                                }
                            )
                            self._snp[
                                "FutureDataUpdateTimeStamp"
                            ] = FutureDataUpdateTimeStamp
                            self._snp["FuturePrice"] = FuturePrice
                            self._snp["FLtpColor"] = FLtpColor
                            self._snp["immediatePrevFLtp"] = immediatePrevFLtp
                            self._update_max_min_oi(_symbol, _expiry)
                            # print(_print)
                    if _type == "Option":
                        _symbol, _expiry, _cepe, _strike, _seacsof, _t = _decoded
                        if isinstance(_expiry, str):
                            _expiry = dtdt.strptime(_expiry, "%d%m%Y").strftime(
                                "%d-%b-%Y"
                            )
                        if len(_seacsof) == 2:
                            (
                                OptionType,
                                Strike,
                                DataUpdateTimeStamp,
                                Open,
                                High,
                                Low,
                                Close,
                                Volume,
                                AskPrice,
                                AskQty,
                                BidPrice,
                                BidQty,
                                AveragePrice,
                                OpenInterest,
                                Delta,
                                Theta,
                                Vega,
                                Gamma,
                                ImpliedVolatatlity,
                                Vanna,
                                Charm,
                                Speed,
                                Zomma,
                                Color,
                                Volga,
                                Veta,
                            ) = (
                                (
                                    "CE"
                                    if _cepe == "c"
                                    else "PE"
                                    if _cepe == "p"
                                    else ""
                                ),
                                _strike,
                                self._qts_app_ts_decode(+_t[4], _string=True),
                                +_t[5],
                                +_t[6],
                                +_t[7],
                                +_t[8],
                                +_t[9],
                                +_t[10],
                                +_t[11],
                                +_t[12],
                                +_t[13],
                                +_t[14],
                                +_t[15],
                                +_t[16],
                                +_t[17],
                                +_t[18],
                                +_t[19],
                                (+_t[20] * 100),
                                +_t[21],
                                +_t[22],
                                +_t[23],
                                +_t[24],
                                +_t[25],
                                +_t[26],
                                +_t[27],
                            )
                            _print = json.dumps(
                                {
                                    "Symbol": _symbol,
                                    "Expiry": _expiry,
                                    "OptionType": OptionType,
                                    "Strike": Strike,
                                    "DataUpdateTimeStamp": DataUpdateTimeStamp,
                                    "Open": Open,
                                    "High": High,
                                    "Low": Low,
                                    "Close/Ltp": Close,
                                    "Volume": Volume,
                                    "AskPrice": AskPrice,
                                    "AskQty": AskQty,
                                    "BidPrice": BidPrice,
                                    "BidQty": BidQty,
                                    "AveragePrice": AveragePrice,
                                    "OI": OpenInterest,
                                    "Delta": Delta,
                                    "Theta": Theta,
                                    "Vega": Vega,
                                    "Gamma": Gamma,
                                    "IV": ImpliedVolatatlity,
                                    "Vanna": Vanna,
                                    "Charm": Charm,
                                    "Speed": Speed,
                                    "Zomma": Zomma,
                                    "Color": Color,
                                    "Volga": Volga,
                                    "Veta": Veta,
                                }
                            )
                            _ri = np.where(self._snp["index"] == Strike)
                            if "c" == _cepe:
                                self._snp["CallDataUpdateTimeStamp"][
                                    _ri
                                ] = DataUpdateTimeStamp
                                self._snp["CallOpen"][_ri] = Open
                                self._snp["CallHigh"][_ri] = High
                                self._snp["CallLow"][_ri] = Low
                                self._snp["CallClose"][_ri] = Close
                                self._snp["CallLtp"][_ri] = Close
                                self._snp["CallLtpChange"][_ri] = (
                                    Close - self._snp["CallPrevLTP"][_ri]
                                )
                                if self._snp["CallPrevLTP"][_ri] != 0:
                                    self._snp["CallLtpChangePercent"][_ri] = (
                                        Close - self._snp["CallPrevLTP"][_ri]
                                    ) / self._snp["CallPrevLTP"][_ri]
                                else:
                                    self._snp["CallLtpChangePercent"][_ri] = 0.0

                                self._snp["CLtpColor"][_ri] = (
                                    "R"
                                    if (self._snp["immediatePrevCLtp"][_ri] - Close) > 0
                                    else "G"
                                    if (self._snp["immediatePrevCLtp"][_ri] - Close) < 0
                                    else "W",
                                )
                                self._snp["immediatePrevCLtp"][_ri] = Close
                                self._snp["CallPrevLTP"][_ri] = Close
                                self._snp["CallVolume"][_ri] = Volume
                                self._snp["CallAskPrice"][_ri] = AskPrice
                                self._snp["CallAskQty"][_ri] = AskQty
                                self._snp["CallBidPrice"][_ri] = BidPrice
                                self._snp["CallBidQty"][_ri] = BidQty
                                self._snp["CallAveragePrice"][_ri] = AveragePrice
                                self._snp["CallOI"][_ri] = OpenInterest
                                self._snp["CallOIChange"][_ri] = (
                                    OpenInterest - self._snp["CallPrevOI"][_ri]
                                )
                                if self._snp["CallPrevOI"][_ri] != 0:
                                    self._snp["CallOIChangePercent"][_ri] = (
                                        OpenInterest - self._snp["CallPrevOI"][_ri]
                                    ) / self._snp["CallPrevOI"][_ri]
                                else:
                                    self._snp["CallOIChangePercent"][_ri] = 0.0
                                self._snp["CallDeltaChange"][_ri] = (
                                    Delta - self._snp["CallDelta"][_ri]
                                )
                                self._snp["CallDelta"][_ri] = Delta
                                self._snp["CallThetaChange"][_ri] = (
                                    Theta - self._snp["CallTheta"][_ri]
                                )
                                self._snp["CallTheta"][_ri] = Theta
                                self._snp["CallVegaChange"][_ri] = (
                                    Vega - self._snp["CallVega"][_ri]
                                )
                                self._snp["CallVega"][_ri] = Vega
                                self._snp["CallGammaChange"][_ri] = (
                                    Gamma - self._snp["CallGamma"][_ri]
                                )
                                self._snp["CallGamma"][_ri] = Gamma
                                self._snp["CallIV"][_ri] = ImpliedVolatatlity
                                self._snp["CallIVChange"][_ri] = (
                                    ImpliedVolatatlity - self._snp["CallPrevIV"][_ri]
                                )
                                if self._snp["CallPrevIV"][_ri] != 0:
                                    self._snp["CallIVChangePercent"][_ri] = (
                                        ImpliedVolatatlity
                                        - self._snp["CallPrevIV"][_ri]
                                    ) / self._snp["CallPrevIV"][_ri]
                                else:
                                    self._snp["CallIVChangePercent"][_ri] = 0.0
                                self._snp["CallPrevIV"][_ri] = ImpliedVolatatlity
                                self._snp["CallVannaChange"][_ri] = (
                                    Vanna - self._snp["CallVanna"][_ri]
                                )
                                self._snp["CallVanna"][_ri] = Vanna
                                self._snp["CallCharmChange"][_ri] = (
                                    Charm - self._snp["CallCharm"][_ri]
                                )
                                self._snp["CallCharm"][_ri] = Charm
                                self._snp["CallSpeedChange"][_ri] = (
                                    Speed - self._snp["CallSpeed"][_ri]
                                )
                                self._snp["CallSpeed"][_ri] = Speed
                                self._snp["CallZommaChange"][_ri] = (
                                    Zomma - self._snp["CallZomma"][_ri]
                                )
                                self._snp["CallZomma"][_ri] = Zomma
                                self._snp["CallColorChange"][_ri] = (
                                    Color - self._snp["CallColor"][_ri]
                                )
                                self._snp["CallColor"][_ri] = Color
                                self._snp["CallVolgaChange"][_ri] = (
                                    Volga - self._snp["CallVolga"][_ri]
                                )
                                self._snp["CallVolga"][_ri] = Volga
                                self._snp["CallVetaChange"][_ri] = (
                                    Veta - self._snp["CallVeta"][_ri]
                                )
                                self._snp["CallVeta"][_ri] = Veta
                                self._update_max_min_oi(_symbol, _expiry)
                                print(_print)
                            if "p" == _cepe:
                                self._snp["PutDataUpdateTimeStamp"][
                                    _ri
                                ] = DataUpdateTimeStamp
                                self._snp["PutOpen"][_ri] = Open
                                self._snp["PutHigh"][_ri] = High
                                self._snp["PutLow"][_ri] = Low
                                self._snp["PutClose"][_ri] = Close
                                self._snp["PutLtp"][_ri] = Close
                                self._snp["PutLtpChange"][_ri] = (
                                    Close - self._snp["PutPrevLTP"][_ri]
                                )
                                if self._snp["PutPrevLTP"][_ri] != 0:
                                    self._snp["PutLtpChangePercent"][_ri] = (
                                        Close - self._snp["PutPrevLTP"][_ri]
                                    ) / self._snp["PutPrevLTP"][_ri]
                                else:
                                    self._snp["PutLtpChangePercent"][_ri] = 0.0

                                self._snp["PLtpColor"][_ri] = (
                                    "R"
                                    if (self._snp["immediatePrevPLtp"][_ri] - Close) > 0
                                    else "G"
                                    if (self._snp["immediatePrevPLtp"][_ri] - Close) < 0
                                    else "W",
                                )
                                self._snp["immediatePrevPLtp"][_ri] = Close
                                self._snp["PutPrevLTP"][_ri] = Close
                                self._snp["PutVolume"][_ri] = Volume
                                self._snp["PutAskPrice"][_ri] = AskPrice
                                self._snp["PutAskQty"][_ri] = AskQty
                                self._snp["PutBidPrice"][_ri] = BidPrice
                                self._snp["PutBidQty"][_ri] = BidQty
                                self._snp["PutAveragePrice"][_ri] = AveragePrice
                                self._snp["PutOI"][_ri] = OpenInterest
                                self._snp["PutOIChange"][_ri] = (
                                    OpenInterest - self._snp["PutPrevOI"][_ri]
                                )
                                if self._snp["PutPrevOI"][_ri] != 0:
                                    self._snp["PutOIChangePercent"][_ri] = (
                                        OpenInterest - self._snp["PutPrevOI"][_ri]
                                    ) / self._snp["PutPrevOI"][_ri]
                                else:
                                    self._snp["PutOIChangePercent"][_ri] = 0.0
                                self._snp["PutDeltaChange"][_ri] = (
                                    Delta - self._snp["PutDelta"][_ri]
                                )
                                self._snp["PutDelta"][_ri] = Delta
                                self._snp["PutThetaChange"][_ri] = (
                                    Theta - self._snp["PutTheta"][_ri]
                                )
                                self._snp["PutTheta"][_ri] = Theta
                                self._snp["PutVegaChange"][_ri] = (
                                    Vega - self._snp["PutVega"][_ri]
                                )
                                self._snp["PutVega"][_ri] = Vega
                                self._snp["PutGammaChange"][_ri] = (
                                    Gamma - self._snp["PutGamma"][_ri]
                                )
                                self._snp["PutGamma"][_ri] = Gamma
                                self._snp["PutIV"][_ri] = ImpliedVolatatlity
                                self._snp["PutIVChange"][_ri] = (
                                    ImpliedVolatatlity - self._snp["PutPrevIV"][_ri]
                                )
                                if self._snp["PutPrevIV"][_ri] != 0:
                                    self._snp["PutIVChangePercent"][_ri] = (
                                        ImpliedVolatatlity - self._snp["PutPrevIV"][_ri]
                                    ) / self._snp["PutPrevIV"][_ri]
                                else:
                                    self._snp["PutIVChangePercent"][_ri] = 0.0
                                self._snp["PutPrevIV"][_ri] = ImpliedVolatatlity
                                self._snp["PutVannaChange"][_ri] = (
                                    Vanna - self._snp["PutVanna"][_ri]
                                )
                                self._snp["PutVanna"][_ri] = Vanna
                                self._snp["PutCharmChange"][_ri] = (
                                    Charm - self._snp["PutCharm"][_ri]
                                )
                                self._snp["PutCharm"][_ri] = Charm
                                self._snp["PutSpeedChange"][_ri] = (
                                    Speed - self._snp["PutSpeed"][_ri]
                                )
                                self._snp["PutSpeed"][_ri] = Speed
                                self._snp["PutZommaChange"][_ri] = (
                                    Zomma - self._snp["PutZomma"][_ri]
                                )
                                self._snp["PutZomma"][_ri] = Zomma
                                self._snp["PutColorChange"][_ri] = (
                                    Color - self._snp["PutColor"][_ri]
                                )
                                self._snp["PutColor"][_ri] = Color
                                self._snp["PutVolgaChange"][_ri] = (
                                    Volga - self._snp["PutVolga"][_ri]
                                )
                                self._snp["PutVolga"][_ri] = Volga
                                self._snp["PutVetaChange"][_ri] = (
                                    Veta - self._snp["PutVeta"][_ri]
                                )
                                self._snp["PutVeta"][_ri] = Veta
                                self._update_max_min_oi(_symbol, _expiry)
                                print(_print)
                    self._fpdq.task_done()
            except Exception as e:
                print(
                    f"An Exception {e} has Occurred in Process Decoded Packet Thread..."
                )
                self._fpdq.task_done()
                raise e
                # continue

    def _check_n_return_file_path(self, _file_path):
        try:
            if (
                os.path.exists(os.path.join(os.getcwd(), _file_path))
                and os.path.isfile(os.path.join(os.getcwd(), _file_path))
            ) and not os.path.isdir(
                os.path.join(os.getcwd(), _file_path)
            ):  # noqa: E501
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
            header=True,
            expand="table",
        )

    def _on_init_get_option_chain(self, _symbol: str = None, _expiry: str = None):
        pythoncom.CoInitialize()
        self._update_instrument_records()
        sleep(2)
        while True:
            try:
                if not _symbol and not _expiry:
                    _symbol, _expiry = self._get_instrument_expiry()
                _instrument = f'{_symbol}:{_expiry.strftime("%d%m%Y")}'
                # print(f'{_instrument}')
                break
            except AttributeError:
                continue
        self.to_be_subscribed_tokens.append([_symbol, _expiry])
        self._send_stream_subscribe_request(_symbol, _expiry)

    def _qts_app_ts_decode(self, _timestamp: int, _string=False):
        _dt = dtdt.fromtimestamp(_timestamp)
        if _string:
            return (_dt - dt.timedelta(hours=5, minutes=30)).strftime(
                "%d-%b-%Y %H:%M:%S"
            )  # noqa: E501
        else:
            return _dt - dt.timedelta(hours=5, minutes=30)

    def _ci(self, _column_name: str):
        if self._col is not None:
            _ci = self._col[_column_name]
            return _ci

    def _ri(self, _strike):
        if self._np is not None:
            _ri = np.where(self._np["index"] == _strike)  # [0][0]
            return _ri

    def _get_length(self, _byte_format: list):
        if len(_byte_format) >= 2:
            return sum(
                list(
                    map(
                        lambda x: sum(
                            list(
                                map(
                                    lambda x: self.FORMAT_CHARACTERS[x]["Standard_Size"]
                                    if x not in ["s", "x", "p", "P"]
                                    else 0,
                                    x,
                                )
                            )  # noqa: E501
                        )
                        if isinstance(x, str)
                        else x,
                        _byte_format,
                    )
                )
            )  # noqa: E501
        else:
            return sum(
                list(
                    map(
                        lambda x: self.FORMAT_CHARACTERS[x]["Standard_Size"]
                        if x not in ["s", "x", "p", "P"]
                        else 0,
                        _byte_format[0],
                    )
                )
            )  # noqa: E501

    def _unpack(self, _bin, _end, _start=0, _byte_format="ch"):
        """Unpack binary data as unsgined interger."""
        return struct.unpack("<" + _byte_format, _bin[_start:_end])

    def _pack(self, _bin, _byte_format="ch"):
        """Unpack binary data as unsgined interger."""
        return struct.pack("<" + _byte_format, *_bin)

    def _set_process_and_join_at(self):
        if len(self.subscribed_tokens) > 0:
            _symbol, _expiry = self.subscribed_tokens[0].split(":")[0], dtdt.strptime(
                self.subscribed_tokens[0].split(":")[-1],
                "%d%m%Y",
            ).strftime("%d-%b-%Y")
            (self._process_at,) = self._instrument_records.at[
                self._instrument_records.query(
                    "SymbolName== @_symbol & ExpiryDate== @_expiry"
                )["Strikes"].index[0],
                "Strikes",
            ]
            self._join_at = self._process_at * 2
            print(self._process_at, self._join_at)

    def _decode_packets(self):
        while True:
            try:
                _bin = self._wsq.get_nowait()
                # print(_bin)
            except queue.Empty:
                _bin = None
                continue
            try:
                if _bin is not None and len(_bin) >= 2:
                    j = self._get_length(_byte_format=["ch"])
                    initial_data = self._unpack(_bin, _end=j, _byte_format="ch")
                    message_type, string_length = (
                        initial_data[0].decode("utf-8"),
                        initial_data[1],
                    )
                    # print(message_type, string_length)
                    if "m" == message_type:
                        j = self._get_length(_byte_format=["chc"])
                        t = self._unpack(_bin, _end=j, _byte_format="chc")
                        s_u = t[-1].decode("utf-8")
                        if s_u == "s":
                            _symbol, _expiry = (
                                self.to_be_subscribed_tokens[0][0],
                                (self.to_be_subscribed_tokens[0][-1]).strftime(
                                    "%d%m%Y"
                                ),
                            )
                            self.subscribed_tokens.append(f"{_symbol}:{_expiry}")
                            self.to_be_subscribed_tokens.clear()
                            print(
                                f"-- {_symbol}:{_expiry} Subscribed Successfully --"
                            )  # noqa: E501
                            (
                                self._subscribed_successful,
                                self._unsubscribed_successful,
                            ) = (
                                True,
                                False,
                            )
                            self._set_process_and_join_at()
                        elif s_u == "u":
                            _symbol, _expiry = (
                                self.subscribed_tokens[0].split(":")[0],
                                self.subscribed_tokens[0].split(":")[-1],
                            )
                            self.subscribed_tokens.remove(
                                f"{_symbol}:{_expiry}"
                            )  # noqa: E501
                            print(
                                f"-- {_symbol}:{_expiry} Unsubscribed Successfully --"
                            )  # noqa: E501
                            (
                                self._subscribed_successful,
                                self._unsubscribed_successful,
                            ) = (
                                False,
                                True,
                            )
                        else:
                            print(f"-- {s_u} --")
                    elif "c" == message_type:
                        j = self._get_length(_byte_format=["chc"])
                        z = self._unpack(_bin, _end=j, _byte_format="chc")
                        # print(z)
                    elif "o" == message_type:
                        j = self._get_length(_byte_format=["chh"])
                        print("-- stream connection open --")
                        z = self._unpack(_bin, _end=j, _byte_format="chh")
                        # print(z)
                    elif "e" == message_type:
                        j = self._get_length(_byte_format=["chhccc"])
                        print("-- stream connection open --")
                        z = self._unpack(_bin, _end=j, _byte_format="chhccc")
                        # print(z)
                        if 1003 == z[2]:
                            print(
                                "Data failed due to max number of instruments exceeded",
                                "Error",
                            )  # noqa: E501
                            self.close()
                        if 1004 == z[2]:
                            print(
                                "----- log out ---", "Session expired", "Signin Again"
                            )  # noqa: E501
                        if 1005 == z[2]:
                            print(
                                "-- connection open another place ----",
                                "Connection already open ",
                            )
                            self.close()
                    else:
                        if self.testing:
                            _symbol, _expiry = (
                                self.to_be_subscribed_tokens[0][0],
                                (self.to_be_subscribed_tokens[0][-1]).strftime(
                                    "%d%m%Y"
                                ),
                            )

                        else:
                            _symbol, _expiry = (
                                self.subscribed_tokens[0].split(":")[0],
                                self.subscribed_tokens[0].split(":")[-1],
                            )
                        _byte_format = ["chh", string_length, "s"]
                        j = self._get_length(_byte_format)
                        t = self._unpack(
                            _bin,
                            _end=j,
                            _byte_format="".join(list(map(str, _byte_format))),
                        )  # noqa: E501
                        t = t[3].decode("utf-8")
                        # print(t)
                        symbol_expiry_and_cepe_strike_or_future = t.split("|")
                        symbol_expiry = symbol_expiry_and_cepe_strike_or_future[0]
                        symbol, expiry = (
                            symbol_expiry.split(":")[0],
                            symbol_expiry.split(":")[-1],
                        )
                        cepe_strike_or_future = (
                            symbol_expiry_and_cepe_strike_or_future[-1]
                            if symbol_expiry_and_cepe_strike_or_future[-1]
                            else ""
                        )
                        if (
                            "x" != cepe_strike_or_future and "" != cepe_strike_or_future
                        ):  # noqa: E501
                            cepe, strike = (
                                cepe_strike_or_future.split(":")[0],
                                cepe_strike_or_future.split(":")[-1],
                            )
                        # print(symbol_expiry, cepe_strike_or_future)
                    if (
                        "g" == message_type
                        and "x" == cepe_strike_or_future
                        and symbol == _symbol
                        and expiry == _expiry
                    ):  # noqa: E501
                        # print("dataPriceWithoutStrike -- without strike --")
                        _byte_format = ["chh", string_length, "siffffqfififqc"]
                        j = self._get_length(_byte_format)
                        t = self._unpack(
                            _bin,
                            _end=j,
                            _byte_format="".join(list(map(str, _byte_format))),
                        )  # noqa: E501
                        seacsof = symbol_expiry_and_cepe_strike_or_future
                        self._ipdq.put_nowait(("Future", (symbol, expiry, seacsof, t)))
                    elif (
                        "g" == message_type
                        and "x" != cepe_strike_or_future
                        and "" != cepe_strike_or_future  # noqa: E501
                        and symbol == _symbol
                        and expiry == _expiry
                    ):  # noqa: E501
                        # print("dataPriceWithStrike -- with strike --")
                        _byte_format = [
                            "chh",
                            string_length,
                            "siffffqfififqffffffffffff",
                        ]
                        j = self._get_length(_byte_format)
                        t = self._unpack(
                            _bin,
                            _end=j,
                            _byte_format="".join(list(map(str, _byte_format))),
                        )  # noqa: E501
                        seacsof = symbol_expiry_and_cepe_strike_or_future
                        try:
                            strike = int(strike)
                        except ValueError:
                            strike = float(strike)
                        self._ipdq.put_nowait(
                            ("Option", (symbol, expiry, cepe, strike, seacsof, t))
                        )
                else:
                    print("Unable to decode packet, packet length is less than 2")
                self._wsq.task_done()
            except Exception as e:
                self._wsq.task_done()
                print(f"An Exception {e} Occured in Decode Packet Thread...")
                continue

    def _subscribe_packets_formated(self, _symbol: str, _expiry: str):
        _instrument = f"{_symbol.upper()}:{_expiry}"
        # print(_instrument)
        i = len(_instrument)
        _byte_format = ["chccc", i, "s"]
        _bin = [
            bytes("s", "utf-8"),
            3 + i,
            bytes("c", "utf-8"),
            bytes("g", "utf-8"),
            bytes("n", "utf-8"),
            bytes(_instrument, "utf-8"),
        ]
        # print(_bin)
        return self._pack(
            _bin, _byte_format="".join(list(map(str, _byte_format)))
        )  # noqa: E501

    def _unsubscribe_packets_formated(self, _symbol: str, _expiry: str):
        _instrument = f"{_symbol.upper()}:{_expiry}"
        e = len(_instrument)
        _byte_format = ["chc", e, "s"]
        _bin = [
            bytes("u", "utf-8"),
            1 + e,
            bytes("c", "utf-8"),
            bytes(_instrument, "utf-8"),
        ]
        return self._pack(
            _bin, _byte_format="".join(list(map(str, _byte_format)))
        )  # noqa: E501

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

    def _compute_chunksize(self, _iterable_size: int, _pool_size: int):
        _chunksize, _remainder = divmod(_iterable_size, _pool_size * 4)
        if _remainder:
            _chunksize += 1
        return _chunksize

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
