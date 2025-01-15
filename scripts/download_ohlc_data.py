import dateutil
import duckdb
import toml
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
import pytz

from ohlc_downloader.ohlc_downloaders import *

try:
    from proprietary_setup import ohlc_downloader
except ImportError:
    ohlc_downloader = IOhlcDownloader()

ny_tz = pytz.timezone('America/New_York')
utc_tz = pytz.utc

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

config = toml.load('../config.toml')

motherduck_token = config['motherduck']['token']
duckdb_conn = duckdb.connect(f'md:my_db?motherduck_token={motherduck_token}')

mongo_client = MongoClient(config['mongo']['remote'])
minute_mongo_ohlc_dest_collection = mongo_client['html_downloads']['minute_ohlc_data']

batch_downloader = BarchartBatchDownloader()


def get_symbol_with_month_list(offset, limit) -> list[tuple[str, dt.date]]:
    query = f"""
    WITH
      q1 AS (
        SELECT 
          publish_time_NY,
          (financial_event_with_symbols->>'$[*].symbol.symbol').unnest().upper() AS symbol,
          (financial_event_with_symbols->>'$[*].symbol.stock_exchanges[*]').unnest().upper() AS exchange
        FROM llm_feature_extract_date_ny
      ),
      
     q2 AS (
        SELECT
        distinct 
          symbol,
          date_trunc('month', publish_time_NY) AS truncated_month
        FROM q1
        WHERE 
          symbol IN (SELECT * FROM clean_symbols)
          AND exchange IN ('NASDAQ')
      )
    
    SELECT symbol, truncated_month
    FROM q2
    ORDER BY (symbol, truncated_month)
    OFFSET {offset}
    limit {limit}
    """

    symbol_month_list = duckdb_conn.execute(query).fetchall()

    return symbol_month_list


def get_saved_symbol_month_pairs(symbol_month_pairs: list[tuple[str, dt.date]]) -> list[tuple[str, dt.date]]:
    results = []
    for symbol, timestamp in symbol_month_pairs:
        start_date_ny = dt.datetime.combine(timestamp.replace(day=1), dt.time.min).astimezone(ny_tz)
        end_date_ny = ((start_date_ny + dateutil.relativedelta.relativedelta(months=1) - dt.timedelta(microseconds=1))
                       .astimezone(ny_tz))

        start_date_utc = start_date_ny.astimezone(utc_tz)
        end_date_utc = end_date_ny.astimezone(utc_tz)

        query = {
            'symbol': symbol,
            'timestamp': {'$gte': start_date_utc, '$lt': end_date_utc}
        }

        doc = minute_mongo_ohlc_dest_collection.find_one(query)
        if doc:
            results.append((symbol, start_date_ny))

    return results


def iterate_df_with_column_names(df: pl.DataFrame):
    for row in df.iter_rows():
        yield list(zip(df.columns, row))


async def main():
    batch_size = 10
    offset = 0

    logger.info('Starting')

    async with ohlc_downloader as downloader:
        while True:

            logger.info(f'Running batch with offset={offset}')

            symbol_with_month_list = get_symbol_with_month_list(offset, batch_size)
            if not symbol_with_month_list:
                break

            saved_pairs = get_saved_symbol_month_pairs(symbol_with_month_list)

            if len(saved_pairs) > 0:
                logger.info(f'Found {len(saved_pairs)} already saved pairs')

            non_saved_pairs = set(symbol_with_month_list) - set(saved_pairs)

            if not non_saved_pairs:
                offset += batch_size
                continue

            ohlc_download_requests = []

            for symbol, month_dt in non_saved_pairs:
                dt_start, dt_end = month_dt, month_dt + relativedelta(months=1)

                ohlc_download_request = OhlcDownloadRequest(
                    symbol, dt_start, dt_end
                )
                logger.info(f'Adding [{symbol}, {month_dt}] to download list')
                ohlc_download_requests.append(ohlc_download_request)

            logger.info(f'Downloading {len(ohlc_download_requests)} symbols')

            download_results = await batch_downloader.download_batch(downloader, ohlc_download_requests)

            if not download_results:
                offset += batch_size
                continue

            mongo_insert_ops = []

            logger.info(f'Inserting into mongo {len(download_results)} results')

            for result in download_results:
                for row_with_col in iterate_df_with_column_names(result.df):
                    row_dict = dict(row_with_col)
                    row_dict['symbol'] = result.symbol
                    insert_op = InsertOne(row_dict)
                    mongo_insert_ops.append(insert_op)

            try:
                minute_mongo_ohlc_dest_collection.bulk_write(
                    mongo_insert_ops,
                    ordered=False
                )
            except BulkWriteError as bwe:
                for error in bwe.details['writeErrors']:
                    if error['code'] == 11000:  # Duplicate key error code
                        continue  # ignore duplicate key errors
                    raise bwe

            offset += batch_size

    logger.info('Done')


def main2():
    symbol = 'AAPL'
    start_dt = dt.datetime(2010, 1, 7, 10, 30, 00)
    end_dt = dt.datetime(2010, 1, 8, 10, 30, 00)

    ohlc_download_request = OhlcDownloadRequest(symbol, start_dt, end_dt)

    results = asyncio.run(ohlc_downloader.download_batch(ohlc_downloader, [ohlc_download_request]))

    print(results)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
