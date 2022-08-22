import asyncio
from asyncio import AbstractEventLoop
from logging import Logger
from threading import Thread
from typing import Callable

from pika import ConnectionParameters, SelectConnection
from pika.channel import Channel
from pika.exceptions import AMQPConnectionError, ConnectionClosedByClient

from .._logger import _LOGGER
from .base_connection import BaseConnection


class AsyncRabbitMQConnection(BaseConnection):
    _connection: SelectConnection

    def __init__(
        self,
        event_loop: AbstractEventLoop = None,
        on_connection_open: Callable = None,
        on_connection_closed: Callable = None,
        on_connection_stop_called: Callable = None,
        logger: Logger = _LOGGER,
        **kwargs,
    ) -> None:

        super().__init__(logger, **kwargs)

        self._on_connection_open_cb = on_connection_open
        self._on_connection_closed_cb = on_connection_closed
        self._on_connection_stop_called_cb = on_connection_stop_called
        self._event_loop = event_loop if event_loop else asyncio.get_event_loop()
        self._closing = False

    def close_connection(self):
        if self._connection.is_closing or self._connection.is_closed:
            self._logger.info("Connection is closing or already closed")
        else:
            self._logger.info("Closing connection")
            self._connection.close()

    def on_connection_open(self, connection: SelectConnection):
        self._connection = connection
        if self._on_connection_open_cb:
            self._on_connection_open_cb()

    def on_connection_open_error(self, _unused_connection: SelectConnection, err: AMQPConnectionError):
        """This method is called by pika if the connection to RabbitMQ can't be established.
        :param _unused_connection: The connection
        :param err: The error
        """
        if not isinstance(err, AMQPConnectionError):
            self._logger.error(f"Connection open failed: {err}")
            return

        msg = f"fConnection open to {self._host}:{self._port} failed: "
        for failure in err.args:
            msg += ", ".join([str(e.exception) for e in failure.exceptions])

        self._logger.error(msg)
        raise err

    def connect(self):
        parameters = ConnectionParameters(host=self._host, port=self._port, credentials=self._credentials)
        self._connection = SelectConnection(
            parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )
        Thread(target=self._connection.ioloop.start).start()  # noqa

    def on_connection_closed(self, _unused_connection: SelectConnection, reason: BaseException):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly.
        """
        if self._on_connection_closed_cb:
            self._on_connection_closed_cb()

        # if self._closing:
        self._connection.ioloop.stop()

        if isinstance(reason, ConnectionClosedByClient) and reason.reply_text == "Normal shutdown":
            self._logger.info(f"Connection closed: {reason}")
        else:
            self._logger.warning(f"Connection closed: {reason}")

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the connection with RabbitMQ.
        When RabbitMQ confirms the cancellation, on_cancelok will be invoked by pika, which will then
        closing the channel and connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This exception stops the IOLoop which needs
        to be running for pika to communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if self._on_connection_stop_called_cb:
            self._on_connection_stop_called_cb()

        if self._connection is not None:
            self._logger.info("Closing RabbitMQ connection")
            self._connection.close()
            self._connection.ioloop.stop()
            self._logger.info("RabbitMQ stopped")

    @property
    def create_channel(self) -> Callable[[int, Callable, ...], Channel]:
        """Real signature: create_channel(channel_number=None, on_open_callback=None)"""
        return self._connection.channel
