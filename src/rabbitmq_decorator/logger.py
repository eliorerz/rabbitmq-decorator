import logging
import os


class _Logger:
    ENVIRONMENT_VARIABLE_NAME = "LOGGING_LEVEL"
    DEFAULT_DEBUG_LEVEL = logging.INFO
    __colors_set = False
    __instance = None

    def __init__(self, name: str, level: int = logging.DEBUG):
        self.__name = name
        self.__logger = logging.getLogger(name)
        self.__level = level
        self.__logger.setLevel(level)
        self.__format = logging.Formatter(f"%(levelname)-14s | %(asctime)s | %(name)-10s | "
                                          f"%(funcName)-10s | %(module)s:%(lineno)-5d | %(message)s\033[1;0m")
        self.__add_stdout_handler()
        self.__set_colors()

    @classmethod
    def get_debug_level(cls, name: str):
        level = cls.DEFAULT_DEBUG_LEVEL

        try:
            if cls.ENVIRONMENT_VARIABLE_NAME in os.environ:
                var_name = cls.ENVIRONMENT_VARIABLE_NAME
            else:
                var_name = f"{cls.ENVIRONMENT_VARIABLE_NAME}_{name.upper()}"
            level = int(os.environ[var_name])
        except BaseException:
            """ TypeError, ValueError, KeyError"""
        return level

    @classmethod
    def __set_colors(cls):
        if cls.__colors_set:
            return

        logging.addLevelName(logging.DEBUG, f"\033[1;34m{logging.getLevelName(logging.DEBUG)}")
        logging.addLevelName(logging.INFO, f"\033[1;37m{logging.getLevelName(logging.INFO)}")
        logging.addLevelName(logging.WARNING, f"\033[1;33m{logging.getLevelName(logging.WARNING)}")
        logging.addLevelName(logging.ERROR, f"\033[1;31m{logging.getLevelName(logging.ERROR)}")
        logging.addLevelName(logging.CRITICAL, f"\033[1;35m{logging.getLevelName(logging.CRITICAL)}")
        cls.__colors_set = True

    @property
    def log_name(self):
        return self.__name

    @property
    def logger(self):
        return self.__logger

    def __add_stdout_handler(self):
        """
        Add stdout logger (print to screen)
        If handler already exists nothing happening
        :return: None
        """

        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(self.__level)
        stdout_handler.setFormatter(self.__format)
        self.__logger.addHandler(stdout_handler)


class Logger:

    def __new__(cls, name) -> logging.Logger:
        return _Logger(name, _Logger.get_debug_level(name)).logger


_LOGGER = Logger("rabbitmq-decorator")
