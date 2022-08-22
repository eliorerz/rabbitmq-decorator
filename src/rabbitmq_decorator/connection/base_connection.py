import os
from abc import ABC
from logging import Logger

from pika import PlainCredentials

from .._logger import _LOGGER


class BaseConnection(ABC):
    DEFAULT_RABBITMQ_PORT = 5672

    def __init__(self, logger: Logger = None, **kwargs) -> None:

        self._host = kwargs.get("rabbitmq_host", os.environ.get("RABBITMQ_HOST"))
        self._port = int(kwargs.get("rabbitmq_port", os.environ.get("RABBITMQ_PORT", self.DEFAULT_RABBITMQ_PORT)))
        self._credentials = PlainCredentials(
            username=kwargs.get("rabbitmq_username", os.environ.get("RABBITMQ_USERNAME")),
            password=kwargs.get("rabbitmq_password", os.environ.get("RABBITMQ_PASSWORD")),
        )

        self._connection = None
        self._logger = logger or _LOGGER

    @property
    def create_channel(self):
        return self._connection.channel
