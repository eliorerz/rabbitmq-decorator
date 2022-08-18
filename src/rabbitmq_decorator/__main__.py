import asyncio
import os
from typing import Dict, Any

from .consumer import RabbitMQConsumer, RabbitMQHandler, Exchange, RabbitConsumerInterface


PASSWORD = os.environ.get("RABBITMQ_PASSWORD")
USERNAME = os.environ.get("RABBITMQ_USERNAME")
PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
HOST = os.environ.get("RABBITMQ_HOST", "localhost")

event_loop = asyncio.get_event_loop()


async def main(cls):
    i = 0
    while i < 4:
        await asyncio.sleep(1)
        i += 1
    cls.stop()
    await asyncio.sleep(2)
    cls.start(cls)


consumer = RabbitMQHandler(event_loop=event_loop)


@consumer
class SomeClass(RabbitConsumerInterface):

    def __init__(self, x: int = 1) -> None:
        self.x = x

    @RabbitMQConsumer(Exchange("test_exchange", durable=True), route="*.*.*")
    async def consumer_1(self, body: Dict[str, Any]):
        print("consumer_1", body)

    @RabbitMQConsumer(Exchange("test_exchange", durable=True), route="*.*.*")
    async def consumer_2(self, body: Dict[str, Any]):
        print("consumer_2", body)


if __name__ == '__main__':
    cls = SomeClass()
    cls.start(cls)

    event_loop.run_until_complete(main(cls))

    # asyncio.run(main())
