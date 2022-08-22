import asyncio
import functools
from asyncio import AbstractEventLoop
from logging import Logger
from typing import Callable, List

from pika.amqp_object import Method

from . import RabbitMQConnection
from ._common import DECORATOR_ATTRIBUTE, Exchange
from ._logger import _LOGGER
from .connection import AsyncRabbitMQConnection
from .rabbitmq_consumer import RabbitMQConsumer
from .rabbitmq_producer import RabbitMQProducer


class RabbitMQHandler:
    DEFAULT_RABBITMQ_PORT = 5672

    def __init__(self, event_loop: AbstractEventLoop = None, logger: Logger = _LOGGER, **connection_kwargs) -> None:
        self._consumers: List[RabbitMQConsumer] = []
        self._event_loop = event_loop if event_loop else asyncio.get_event_loop()
        self._consumer_connection = AsyncRabbitMQConnection(
            event_loop, self._on_consumer_connection_open, **connection_kwargs
        )
        self._producer_connection = RabbitMQConnection(logger, **connection_kwargs)
        self._logger = logger
        self._closing = False
        self._consumer_instance = None
        self._producers = []

    def __call__(self, klass):
        """Load all consumers"""
        attributes = [attr for attr_name, attr in klass.__dict__.items()]
        consumers = [func for func in attributes if isinstance(func, Callable) and hasattr(func, DECORATOR_ATTRIBUTE)]
        self._consumers = [getattr(func, DECORATOR_ATTRIBUTE) for func in consumers]

        for consumer_func in consumers:
            consumer = getattr(consumer_func, DECORATOR_ATTRIBUTE)
            self._consumers.append(consumer)
            consumer.set_queue_name(klass.__name__)

        return klass

    def get_new_producer(self, exchange: Exchange = None) -> RabbitMQProducer:
        """
        :param exchange: If exchange is set, this producer will produce message only to the specified exchange
        :return: producer instances
        """
        producer = RabbitMQProducer(self._producer_connection, exchange=exchange, logger=self._logger)
        self._producers.append(producer)
        return producer

    def _on_consumer_channel_closed(self):
        """Call after the channel is closed"""
        if all(userdata.channel.is_closed for userdata in self._consumers):
            self._consumer_connection.close_connection()

    def _on_consumer_connection_open(self):
        for consumer in self._consumers:
            cb = functools.partial(
                consumer.on_channel_open,
                on_channel_closed=self._on_consumer_channel_closed,
                event_loop=self._event_loop,
                consumer_instance=self._consumer_instance,
            )

            self._consumer_connection.create_channel(on_open_callback=cb)

    def start(self, consumer_instance: object):
        self._consumer_instance = consumer_instance
        self._consumer_connection.connect()

    def is_consuming(self):
        return all(c.consuming for c in self._consumers)

    def stop_consume(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer with RabbitMQ.
        When RabbitMQ confirms the cancellation, on_cancelok will be invoked by pika, which will then
        closing the channel and connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This exception stops the IOLoop which needs
        to be running for pika to communicate with RabbitMQ. All the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            self._logger.info("Stopping")
            if self.is_consuming():
                for consumer in self._consumers:
                    self._stop_consuming(consumer)
            else:
                self._consumer_connection.stop()
            self._logger.info("Consumer stopped")

    def _stop_consuming(self, consumer: RabbitMQConsumer):
        """Tell RabbitMQ that you would like to stop consuming by sending the Basic.Cancel RPC command."""
        if consumer.channel:
            self._logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            consumer.basic_cancel(callback=functools.partial(self._consumer_on_cancelok, userdata=consumer))

    def _consumer_on_cancelok(self, _unused_frame: Method, userdata: RabbitMQConsumer):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param _unused_frame: The Basic.CancelOk frame
        :param userdata: Extra user data (consumer)
        """
        self._logger.info(f"RabbitMQ acknowledged the cancellation of the consumer: {userdata.consumer_tag}")
        userdata.close_channel()

    def stop(self):
        """stop all services and connections"""
        self.stop_consume()
        self._producer_connection.close()
