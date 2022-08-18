from dataclasses import dataclass, asdict
from typing import Dict, Any

from pika.exchange_type import ExchangeType


DECORATOR_ATTRIBUTE = "__decorator__"


class RabbitConsumerInterface:
    def start(self, consumer_iface):
        pass

    def stop(self):
        pass


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
