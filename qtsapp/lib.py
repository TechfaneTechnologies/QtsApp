# -*- coding: utf-8 -*-

"""
Created on Mon August 8, 08:09:56 2022
@author: DrJuneMoone
"""

import six
import re
import os
import sys
import time
import json
import queue
import struct
import logging
import threading
import requests
import random as stdrandom
import pythoncom
import win32com.client
import pandas as pd
import numpy as np
import xlwings as xw
import datetime as dt
import multiprocessing as mp
import win32api
import pywintypes
import typing
from threading import Thread, Lock as tLock, RLock as tRLock
from copy import copy, deepcopy
from operator import methodcaller
from pandas.core.base import NoNewAttributesMixin
from pandas.core.generic import functools
from base64 import b64encode, b64decode
from queue import Queue
from numpy import random
from time import sleep
from datetime import datetime as dtdt
from contextlib import closing
from websocket import create_connection, _exceptions
from dotenv import dotenv_values
from datetime import datetime
from twisted.internet import reactor, ssl
from twisted.python import log as twisted_log
from twisted.internet.protocol import ReconnectingClientFactory
from autobahn.twisted.websocket import (
    WebSocketClientProtocol,
    WebSocketClientFactory,
    connectWS,
)
from requests.models import PreparedRequest
from win32com.client import dynamic
from concurrent.futures import ProcessPoolExecutor as PE
from multiprocessing import (
    Queue as mqueue,
    JoinableQueue as jqueue,
    Pool,
    Manager,
    current_process,
    cpu_count,
    Process,
    Lock as mLock,
    RLock as mRLock,
)
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.managers import SharedMemoryManager
from concurrent.futures import ProcessPoolExecutor, as_completed

import tracemalloc

# Magic utility that "redirects" to pythoncomxx.dll
pywintypes.__import_pywin32_system_module__("pythoncom", globals())
log = logging.getLogger(__name__)


def validate_sessions(_api_key, _access_token, _app_version):
    try:
        with closing(
            create_connection(
                f"wss://wsoc.quantsapp.com/?user_id={_api_key}&token={_access_token}&portal=web&version={_app_version}&country=in",
                origin="https://web.quantsapp.com",
                host="wsoc.quantsapp.com",
                header={
                    "User-Agent": requests.get(
                        "https://techfanetechnologies.github.io/latest-user-agent/user_agents.json"
                    ).json()[-2]
                },
            )
        ) as ws_user:
            print(f"Session Validated")
            return True, "Session Validated"
    except _exceptions.WebSocketBadStatusException:
        print("Session Expired, Logging in again.....")
        return False, "Session Expired"


def check_n_return_file_path(_file_path):
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


def is_time_between(begin_time, end_time, check_time=None):
    from datetime import datetime, time

    # If check time is not given, default to current UTC time
    check_time = check_time or datetime.utcnow().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time
    else:  # crosses midnight
        return check_time >= begin_time or check_time <= end_time


def isNowInTimePeriod(startTime, endTime, nowTime):
    if startTime < endTime:
        return nowTime >= startTime and nowTime <= endTime
    else:
        # Over midnight:
        return nowTime >= startTime or nowTime <= endTime


def isMarketTime(testing=False):
    if testing:
        return True
    else:
        return isNowInTimePeriod(
            dt.time(9, 15), dt.time(15, 30), dtdt.now().time()
        ) and (dtdt.now().strftime("%A") not in ["Saturday", "Sunday"])


def update_formula(_method: str, _wb_name: str, _ws_range: str, _formula, **_options):
    pythoncom.CoInitialize()
    _ws = xw.Book(_wb_name).sheets("RawData")
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


def get_wb_name(_wb_name: str):
    _wb_name = (
        check_n_return_file_path(f"{_wb_name}.xlsm")
        if _wb_name != "OptionChain"
        else check_n_return_file_path("OptionChain.xlsm")
    )
    return _wb_name


def update_val(_wb_name: str, _ws_range: str, _value, **_options):
    pythoncom.CoInitialize()
    _wb_name = get_wb_name(_wb_name)
    _ws = xw.Book(_wb_name).sheets("RawData")
    if _options:
        _ws.range(_ws_range).options(**_options).value = _value
    else:
        _ws.range(_ws_range).value = _value


def get_val(_wb_name: str, _ws_range: str, **_options):
    pythoncom.CoInitialize()
    _wb_name = get_wb_name(_wb_name)
    _ws = xw.Book(_wb_name).sheets("RawData")
    if _options:
        return _ws.range(_ws_range).options(**_options).value
    else:
        return _ws.range(_ws_range).value


def clear_val(_wb_name: str, _ws_range: str):
    pythoncom.CoInitialize()
    _wb_name = get_wb_name(_wb_name)
    _ws = xw.Book(_wb_name).sheets("RawData")
    _ws.range(_ws_range).clear_contents()


def get_cell_column(_ws_range: str):
    _cell_name, _column_range = (
        (f"{_ws_range.split(':')[0]}2", _ws_range)
        if ":" in _ws_range
        else (
            _ws_range,
            f"{''.join([i for i in _ws_range if not i.isdigit()])}:{''.join([i for i in _ws_range if not i.isdigit()])}",
        )
    )
    return _cell_name, _column_range


def get_last_empty_row(_wb_name: str, _ws_range: str):
    pythoncom.CoInitialize()
    _wb_name = get_wb_name(_wb_name)
    _ws = xw.Book(_wb_name).sheets("RawData")
    _cell_name, _column_range = get_cell_column(_ws_range)
    _ler = _ws.range(_cell_name).value
    _wb = _ws.range(_column_range)
    _ler = 1 if _ler is None else int((_wb.end("down").address).split("$")[-1])
    return _ler


def append_val(
    _wb_name: str,
    _ws_range: str,
    _ler: int,
    _value,
    **_options,
):
    pythoncom.CoInitialize()
    _wb_name = get_wb_name(_wb_name)
    _ws = xw.Book(_wb_name).sheets("RawData")
    if _options:
        _ws.range(_ws_range).current_region.end("up").offset(_ler, 0).options(
            **_options
        ).value = _value
    else:
        _ws.range(_ws_range).current_region.end("up").offset(_ler, 0).value = _value
