from logging import Logger

from pika.channel import Channel

from ._common import Exchange, RabbitMQMessage
from ._logger import _LOGGER
from .connection import RabbitMQConnection


class RabbitMQProducer:
    _connection: RabbitMQConnection

    def __init__(self, connection: RabbitMQConnection, exchange: Exchange = None, logger: Logger = None):
        self._exchange = exchange
        self._connection = connection
        self._logger = logger or _LOGGER
        self._channel = None
        self._channels = dict()

    def publish(self, message: RabbitMQMessage, exchange: Exchange = None):
        if self._connection.is_closed():
            self._logger.info("Connection is closed. Opening connection")
            self._connection.open()

        exchange = exchange or self._exchange
        channel = self.__get_channel(exchange)
        channel.basic_publish(exchange.exchange, message.routing_key, message.get_encoded_body())

    def __get_channel(self, exchange: Exchange) -> Channel:
        if exchange is None:
            raise ValueError(f"Exchange is mandatory property for publishing a messages using "
                             f"{self.__class__.__name__}")

        if channel := self._channels.get(exchange.exchange):
            return channel

        self._channels[exchange.exchange] = self._connection.create_channel()
        self._channels[exchange.exchange].exchange_declare(**exchange.all())

        return self._channels[exchange.exchange]

    def stop(self):
        for _, channel in self._channels.items():
            if not (channel.is_closed or channel.is_closing):
                channel.close()

        self._connection.close()

        self._channels = {}
        self._connection = None
