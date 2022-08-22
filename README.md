
# RabbitMQ Decorator
* Work in progress

Easily send and receive messages to and from RabbitMQ message broker.


## Installation

```bash
pip install rabbitmq-decorator
```


##  Usage 

### Optional:
```shell
export RABBITMQ_HOST=localhost
export RABBITMQ_PORT=5672
export RABBITMQ_USERNAME=username
export RABBITMQ_PASSWORD=password
```

### Example:

```python
import asyncio
from typing import Dict, Any

from rabbitmq_decorator import RabbitMQHandler, RabbitMQConsumer, RabbitMQProducer, RabbitMQMessage, Exchange

event_loop = asyncio.get_event_loop()

# If environment variables was set
rabbit_handler = RabbitMQHandler(event_loop=event_loop)

# Or if environment variables wasn't set 
rabbit_handler = RabbitMQHandler(
    event_loop=event_loop,
    rabbitmq_host="localhost",
    rabbitmq_port=5672,
    rabbitmq_username="username",
    rabbitmq_password="password"
)


@rabbit_handler
class SomeClass:
    EXCHANGE = Exchange("test_exchange", durable=True)
    _producer: RabbitMQProducer = rabbit_handler.get_new_producer(EXCHANGE)

    def some_unrelated_function(self):
        pass

    @RabbitMQConsumer(EXCHANGE)
    async def consumer_1(self, body: Dict[str, Any]):
        print("I'm getting all the messages on test_exchange")
        self._producer.publish(RabbitMQMessage(routing_key="response.consumer1", body={"status": "Success"}))

    @RabbitMQConsumer(EXCHANGE, route="*.*.*")
    async def consumer_2(self, body: Dict[str, Any]):
        print("I'm getting only the messages on test_exchange with *.*.* as routing-key ")
        response_exchange = Exchange("response_exchanges", durable=True)
        self._producer.publish(
            exchange=response_exchange,
            message=RabbitMQMessage(routing_key="response.consumer2", body={"status": "Success"})
        )


# ================ main ================

async def main():
    """do something"""


if __name__ == '__main__':
    some_class = SomeClass()
    rabbit_handler.start(some_class)
    event_loop.run_until_complete(main())
    rabbit_handler.stop_consume(some_class)
```
