from logging import Logger
from typing import Callable

from pika import ConnectionParameters, BlockingConnection, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

from .base_connection import BaseConnection
from .._logger import _LOGGER


class RabbitMQConnection(BaseConnection):
    _connection: BlockingConnection

    def __init__(self, logger: Logger = None, **kwargs) -> None:
        super().__init__(logger, **kwargs)
        self._connection = None
        self._properties = None
        self._logger = logger or _LOGGER

    @property
    def create_channel(self) -> Callable[[], BlockingChannel]:
        """Real signature: create_channel(self, channel_number=None)"""
        return self._connection.channel

    def open(self, properties: BasicProperties = None):
        self._properties = properties or BasicProperties(content_type="application/json")
        self._connection = BlockingConnection(
            parameters=ConnectionParameters(host=self._host, port=self._port, credentials=self._credentials)
        )

    def is_closed(self) -> bool:
        return self._connection is None or self._connection.is_closed

    def close(self):
        self._connection.close()
        self._connection = None
