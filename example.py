import logging
from qtsapp import *
# logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    # Note: if all greeks icluding 3rd order greeks is required to be streaming in terminal, use QtsAppRun(_stream=True) at amrket time
    # QtsAppRun(_stream=True)
    
    # Note: If You use office-365 then uncomment the below  line QtsAppRun(_stream=True, ws_name="OptionChain - 365")
    # QtsAppRun(_stream=True, ws_name="OptionChain - 365")
    
    # Note: Else Use QtsAppRun() or QtsAppRun(_stream=False) to get the option chain update din the excel sheet every 7-9 secs.
    QtsAppRun()
    
    # Note: If You use office-365 then uncomment the below  line QtsAppRun(_stream=False, ws_name="OptionChain - 365")
    # QtsAppRun(ws_name="OptionChain - 365")
