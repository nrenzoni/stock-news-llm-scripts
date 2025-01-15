import abc

import datetime as dt
import logging

logger = logging.getLogger(__name__)


class IOhlcDownloader(abc.ABC):

    @abc.abstractmethod
    async def download_ohlc(self, symbol: str, start_dt: dt.date, end_dt: dt.date):
        raise NotImplementedError

    @abc.abstractmethod
    async def __aenter__(self) -> "IOhlcDownloader":
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError
