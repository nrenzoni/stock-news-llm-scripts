import logging

import clickhouse_connect
import toml
from pymongo import MongoClient, UpdateOne
from feature_extractor.raw_html_reading import ClickhouseRawHtmlReader

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

config = toml.load('../config.toml')

source_client = MongoClient(config['mongo']['local'])
# source_client = MongoClient(config['mongo']['remote'])
source_db = source_client['html_downloads']
source_collection = source_db['llm_feature_extract_dest']
# source_collection = source_db['llm_feature_extract']

config_clickhouse = config['clickhouse']
ch_client = clickhouse_connect.get_client(
    username=config_clickhouse['username'],
    password=config_clickhouse['password'],
    database=config_clickhouse['database']
)
ch_html_reader = ClickhouseRawHtmlReader(ch_client)

batch_size = 10_000
# batch_size = 1
skip = 0

while True:
    logger.info(f'Reading {batch_size} docs, skip={skip}')
    docs = list(ch_html_reader.read_all(skip, batch_size))
    if not docs:
        break

    update_operations = [
        UpdateOne(
            {'url': doc['url']},
            {
                '$set': {
                    'article_title': doc['articleTitle'],
                    'publish_time': doc['publishTime'],
                    'provided_by': doc['providedBy'],
                    'site_provided_tags': doc['tags'],
                }
            },
            upsert=True
        )
        for doc in docs
    ]

    logger.info(f'Writing {len(update_operations)} docs, skip={skip}')
    source_collection.bulk_write(update_operations, ordered=False)

    skip += batch_size

logger.info('Done')