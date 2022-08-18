import asyncio
import functools
from asyncio import AbstractEventLoop
from logging import Logger
from typing import Callable, List

from pika.amqp_object import Method

from .exchange import DECORATOR_ATTRIBUTE
from .rabbitmq_consumer import RabbitMQConsumer
from src.rabbitmq_decorator._logger import _LOGGER
from src.rabbitmq_decorator.rabbitmq_connection import RabbitMQConnection


class RabbitMQHandler:
    DEFAULT_RABBITMQ_PORT = 5672

    def __init__(self, event_loop: AbstractEventLoop = None, logger: Logger = _LOGGER, **connection_kwargs) -> None:
        self._connection_kwargs = connection_kwargs
        self._consumers: List[RabbitMQConsumer] = []
        self._event_loop = event_loop if event_loop else asyncio.get_event_loop()
        self._connection = RabbitMQConnection(
            event_loop=event_loop,
            on_connection_open=self._on_connection_open,
            **self._connection_kwargs
        )
        self._logger = logger
        self._closing = False
        self._consumer_instance = None

    def __call__(self, klass):
        attributes = [attr for attr_name, attr in klass.__dict__.items()]
        consumers = [func for func in attributes if isinstance(func, Callable) and hasattr(func, DECORATOR_ATTRIBUTE)]
        self._consumers = [getattr(func, DECORATOR_ATTRIBUTE) for func in consumers]

        return klass

    def on_channel_closed(self):
        """Call after the channel is closed"""
        if all(userdata.channel.is_closed for userdata in self._consumers):
            self._connection.close_connection()

    def _on_connection_open(self):
        for consumer in self._consumers:
            cb = functools.partial(
                consumer.on_channel_open,
                on_channel_closed=self.on_channel_closed,
                event_loop=self._event_loop,
                consumer_instance=self._consumer_instance,
            )

            self._connection.create_channel(on_open_callback=cb)

    def start_consume(self, consumer_instance):
        self._consumer_instance = consumer_instance
        self._connection.connect()

    def is_consuming(self):
        return all(c.consuming for c in self._consumers)

    def stop_consume(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer with RabbitMQ.
        When RabbitMQ confirms the cancellation, on_cancelok will be invoked by pika, which will then
        closing the channel and connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This exception stops the IOLoop which needs
        to be running for pika to communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            self._logger.info("Stopping")
            if self.is_consuming():
                for consumer in self._consumers:
                    self.stop_consuming(consumer)
            else:
                self._connection.stop()
            self._logger.info("Stopped")

    def stop_consuming(self, consumer: RabbitMQConsumer):
        """Tell RabbitMQ that you would like to stop consuming by sending the Basic.Cancel RPC command.
        """
        if consumer.channel:
            self._logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            consumer.basic_cancel(callback=functools.partial(self.on_cancelok, userdata=consumer))

    def on_cancelok(self, _unused_frame: Method, userdata: RabbitMQConsumer):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param _unused_frame: The Basic.CancelOk frame
        :param userdata: Extra user data (consumer)
        """
        self._logger.info(f"RabbitMQ acknowledged the cancellation of the consumer: {userdata.consumer_tag}")
        userdata.close_channel()
