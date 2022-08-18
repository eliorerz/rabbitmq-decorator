import json
from asyncio.events import AbstractEventLoop
from threading import Thread
from typing import Callable

from pika import BasicProperties
from pika.amqp_object import Method
from pika.channel import Channel
from pika.exceptions import ChannelClosedByClient
from pika.spec import Basic

from .exchange import Exchange, DECORATOR_ATTRIBUTE
from src.rabbitmq_decorator.exceptions import InvalidConsumerFunctionError
from src.rabbitmq_decorator._logger import _LOGGER


class MessageDecodingMethods:
    JSON = json.loads
    BYTES = bytes
    STRING = lambda msg: msg.decode()  # noqa


class RabbitMQConsumer:
    DEFAULT_PREFETCH_COUNT = 1

    def __init__(self, exchange: Exchange, route: str = "#", decoding_method=MessageDecodingMethods.JSON):
        self._exchange = exchange
        self._routing_key = route
        self._queue_name = None
        self._function = None
        self.channel: Channel = None
        self.prefetch_count = self.DEFAULT_PREFETCH_COUNT
        self.consumer_tag = None
        self._was_consuming = False
        self._on_channel_closed = None
        self._consumer_instance = None
        self._thread = None
        self._event_loop = None
        self._decoding_method = decoding_method
        self.consuming = False

    def __call__(self, function: Callable):
        if hasattr(function, DECORATOR_ATTRIBUTE):
            raise InvalidConsumerFunctionError(f"Cannot use function with attribute {DECORATOR_ATTRIBUTE} as rabbitmq "
                                               f"consumer")

        setattr(function, DECORATOR_ATTRIBUTE, self)
        self._function = function
        self._queue_name = function.__name__
        return function

    @property
    def exchange(self) -> Exchange:
        return self._exchange

    @property
    def queue_name(self) -> str:
        return self._queue_name

    @property
    def routing_key(self) -> str:
        return self._routing_key

    @property
    def routing_key(self) -> str:
        return self._routing_key

    def on_channel_open(
            self,
            channel: Channel,
            on_channel_closed: Callable,
            event_loop: AbstractEventLoop,
            consumer_instance: object
    ):
        self.channel = channel
        self._event_loop = event_loop
        self._on_channel_closed = on_channel_closed
        self._consumer_instance = consumer_instance

        self.add_on_channel_close_callback()
        # for consumer in self._consumers:
        self.setup_exchange()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        _LOGGER.info("Adding channel close callback")
        self.channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel: Channel, reason: BaseException):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param channel: The closed channel
        :param reason: why the channel was closed
        """
        log_message = f"Channel {channel} was closed: {reason}"
        if isinstance(reason, ChannelClosedByClient) and reason.reply_text == "Normal shutdown":
            _LOGGER.info(log_message)
        else:
            _LOGGER.warning(log_message)

        if self.channel.is_closed:
            self.consuming = False

        self._on_channel_closed()

    def setup_exchange(self):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        """
        exchange_name = self.exchange.exchange
        _LOGGER.info(f"Declaring exchange: {exchange_name}")
        self.channel.exchange_declare(callback=self.on_exchange_declareok, **self.exchange.all())

    def on_exchange_declareok(self, _unused_frame: Method, ):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC command.
        :param _unused_frame: Exchange.DeclareOk response frame
        """
        _LOGGER.info(f"Exchange declared: {self.exchange}")
        self.setup_queue()

    def setup_queue(self):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC command.
        When it is complete, the on_queue_declareok method will be invoked by pika.
        """
        _LOGGER.info(f"Declaring queue {self.queue_name}")
        self.channel.queue_declare(queue=self.queue_name, callback=self.on_queue_declareok)

    def on_queue_declareok(self, _unused_frame: Method):
        """Method invoked by pika when the Queue.Declare RPC call made in setup_queue has completed.
        In this method we will bind the queue and exchange together with the routing key by issuing the Queue.
        Bind RPC command. When this command is complete, the on_bindok method will be invoked by pika.
        :param _unused_frame: The Queue.DeclareOk frame
        """
        _LOGGER.info(f"Binding {self.exchange.exchange} to {self.queue_name} with {self.routing_key}")
        self.channel.queue_bind(self.queue_name, self.exchange.exchange, routing_key=self.routing_key,
                                callback=self.on_bindok)

    def on_bindok(self, _unused_frame: Method):
        """Invoked by pika when the Queue.Bind method has completed.
        At this point we will set the prefetch count for the channel.
        :param _unused_frame: The Queue.BindOk response frame
        """
        _LOGGER.info(f"Queue bound: {self.queue_name}")
        self.set_qos()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        self.channel.basic_qos(prefetch_count=self.prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame: Method):
        """Invoked by pika when the Basic.QoS method has completed. At this point we will start consuming
        messages by calling start_consuming which will invoke the needed RPC commands to start the process.
        :param _unused_frame: The Basic.QosOk response frame
        """
        _LOGGER.info(f"QOS set to: {self.prefetch_count}")

        self._thread = Thread(target=self.start_consuming)
        self._thread.start()

    def start_consuming(self):
        """This method sets up the consumer by first calling add_on_cancel_callback so that the object
        is notified if RabbitMQ cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the consumer with RabbitMQ.
        We keep the value to use it when we want to cancel consuming. The on_message method is passed in as
        a callback pika will invoke when a message is fully received.
        """
        _LOGGER.info("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        self.consumer_tag = self.channel.basic_consume(
            self.queue_name,
            self.get_callback(self._consumer_instance, self._event_loop)
        )
        self._was_consuming = True
        self.consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer for some reason.
        If RabbitMQ does cancel the consumer, on_consumer_cancelled will be invoked by pika.
        """
        _LOGGER.info("Adding consumer cancellation callback")
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame: Method):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer receiving messages.
        :param method_frame: The Basic.Cancel frame
        """
        _LOGGER.info(f"Consumer was cancelled remotely, shutting down: {method_frame}")
        if self.channel:
            self.channel.close()

    def get_callback(self, parent: object, event_loop: AbstractEventLoop):
        def on_message(channel: Channel, basic_deliver: Basic.Deliver, properties: BasicProperties, body: bytes):
            """Invoked by pika when a message is delivered from RabbitMQ. The channel is passed for your convenience.
            The basic_deliver object that is passed in carries the exchange, routing key, delivery tag and a redelivered
            flag for the message. The properties passed in is an instance of BasicProperties with the message properties
            and the body is the message that was sent.
            :param channel: The channel object
            :param basic_deliver: basic_deliver method
            :param properties: properties
            :param bytes body: The message body
            """
            try:
                decoding_method = self._decoding_method if self._decoding_method else lambda x: x
                _LOGGER.debug(f"Received message # {basic_deliver.delivery_tag} from {properties.app_id}: {body}")
                event_loop.create_task(self._function(parent, body=decoding_method(body)))
                channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)
            except BaseException as e:
                self._on_handle_message_exception(channel, basic_deliver, e)

        return on_message

    def _on_handle_message_exception(self, channel: Channel, basic_deliver: Basic.Deliver, exception: BaseException):
        _LOGGER.error(f"{exception.__class__.__name__}: Error in callback {self._function.__name__}, delivery_tag={basic_deliver.delivery_tag}, "
                      f"{exception}")
        channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)

    def basic_cancel(self, callback=None):
        self.channel.basic_cancel(self.consumer_tag, callback)

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the Channel.Close RPC command."""
        self.consuming = False
        _LOGGER.info("Closing the channel")
        self.channel.close()
