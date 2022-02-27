import logging
from logging import Logger


class LoggingMixin:
    @property
    def log(self) -> Logger:
        """
        Returns a logger.
        """
        try:
            return self._log  # type: ignore
        except AttributeError:
            self._log = logging.getLogger(self.__class__.__module__ + "." + self.__class__.__name__)
            return self._log
