# -*- coding: utf-8 -*-

"""
Created on Mon August 8, 08:09:56 2022
@author: DrJuneMoone
"""

import logging
from qtsapp import *

##############################################################################
# If You Need Logging, uncomment The BeloW Line
# logging.basicConfig(level=logging.DEBUG)

global qtsapp, stream, _is_running_live, first_connect
_is_running_live = False
first_connect = True

##############################################################################
# If Not Using Ms.Office 365
wb_name = "OptionChain"

# If You Are Using Ms.Office 365 Then Uncomment The Below Line.
# wb_name = "OptionChain365"

##############################################################################
# Note: If All Greeks Icluding 3rd Order Greeks Is Required To Be Streaming
# In Excel And Terminal, Then Use stream=True, OtherWise To fet 7-9 Second
# SnapShot of Option Chain use stream=False
stream = True

##############################################################################
if __name__ == "__main__":
    EnchantQtsApp(
        stream=stream,
        _is_running_live=_is_running_live,
        first_connect=first_connect,
        wb_name=wb_name,
    )
