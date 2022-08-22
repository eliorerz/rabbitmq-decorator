import json
from dataclasses import dataclass, asdict
from typing import Dict, Any

from pika.exchange_type import ExchangeType

DECORATOR_ATTRIBUTE = "__consumer_decorator__"


@dataclass
class Exchange:
    exchange: str
    exchange_type: str = ExchangeType.topic
    passive: bool = False
    durable: bool = False
    auto_delete: bool = False
    internal: bool = False
    arguments: bool = None

    def all(self) -> Dict[str, Any]:
        return asdict(self)


class MessageDecodingMethods:
    JSON = json.loads
    BYTES = bytes
    STRING = lambda msg: msg.decode()  # noqa


class MessageEncodingMethods:
    BYTES = bytes
    STRING = lambda msg: msg.encode()  # noqa
    JSON = lambda s: json.dumps(s).encode()  # noqa


@dataclass
class RabbitMQMessage:
    routing_key: str
    body: object

    def get_encoded_body(self) -> bytes:
        if isinstance(self.body, bytes):
            return self.body

        if isinstance(self.body, dict):
            return MessageEncodingMethods.JSON(self.body)

        if isinstance(self.body, dict):
            return MessageEncodingMethods.STRING(self.body)
