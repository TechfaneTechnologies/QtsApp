# -*- coding: utf-8 -*-

"""
Created on Mon August 8, 08:09:56 2022
@author: DrJuneMoone
"""


from qtsapp.lib import *


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
            log.debug(
                "last pong was {} seconds back.".format(
                    time.time() - self._last_pong_time
                )
            )

        self._last_pong_time = time.time()

        if self.factory.debug:
            log.debug("pong => {}".format(response))

    # """
    # Custom helper and exposed methods.
    # """
    # drop existing connection to avoid ghost connection
    # self.dropConnection(abort=True)
