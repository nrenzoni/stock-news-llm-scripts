import abc
import dataclasses
import io

import datetime as dt
import polars as pl
import logging

from interfaces.ohlc_downloader import IOhlcDownloader

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class OhlcDownloadRequest:
    symbol: str
    start_dt: dt.date
    end_dt: dt.date


@dataclasses.dataclass
class OhlcDownloadResult:
    symbol: str
    start_dt: dt.date
    end_dt: dt.date
    df: pl.DataFrame


def convert_to_pl_df(barchart_ohlc_data: str):
    if barchart_ohlc_data.strip() == '':
        raise ValueError('No data')

    data_io = io.StringIO(barchart_ohlc_data)

    ts_column_name = 'timestamp'

    df = pl.read_csv(
        data_io,
        has_header=False,
        try_parse_dates=True,
        new_columns=[ts_column_name, 'day_of_month', 'open', 'high', 'low', 'close', 'volume'],
        schema={
            ts_column_name: pl.Datetime,
            'day_of_month': pl.Int8,
            'open': pl.Float64,
            'high': pl.Float64,
            'low': pl.Float64,
            'close': pl.Float64,
            'volume': pl.Int64
        }
    ).with_columns(
        pl.col(ts_column_name).dt.replace_time_zone('America/New_York'),
    )

    return df


class IBatchDownloader(abc.ABC):
    @abc.abstractmethod
    async def download_batch(self, downloader: IOhlcDownloader, ohlc_download_requests: list[OhlcDownloadRequest]):
        raise NotImplementedError


class BarchartBatchDownloader(IBatchDownloader):
    async def download_batch(
            self,
            downloader: IOhlcDownloader,
            ohlc_download_requests: list[OhlcDownloadRequest]
    ):
        results = []

        for ohlc_download_request in ohlc_download_requests:
            ohlc_val, error_res = await downloader.download_ohlc(
                ohlc_download_request.symbol,
                ohlc_download_request.start_dt,
                ohlc_download_request.end_dt,
            )
            if error_res:
                print(error_res)
                continue

            if ohlc_val.strip() == '':
                logger.info(
                    f'No data for {ohlc_download_request.symbol}, {ohlc_download_request.start_dt}, {ohlc_download_request.end_dt}')
                continue

            if len(ohlc_val) < 100 and 'error' in ohlc_val.lower():
                logger.info(
                    f'Error downloading data for {ohlc_download_request.symbol}, {ohlc_download_request.start_dt.isoformat()}: {ohlc_val}'
                )
                continue

            try:
                df = convert_to_pl_df(ohlc_val)
            except Exception as e:
                logger.warning(
                    f'Error converting to pl df for {ohlc_download_request.symbol}, {ohlc_download_request.start_dt.isoformat()}: {e}'
                )
                continue

            response = OhlcDownloadResult(
                symbol=ohlc_download_request.symbol,
                start_dt=ohlc_download_request.start_dt,
                end_dt=ohlc_download_request.end_dt,
                df=df
            )

            results.append(response)

        return results
