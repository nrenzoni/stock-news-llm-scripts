import logging

import toml
from pymongo import MongoClient

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

config = toml.load('../config.toml')

# Source database
source_client = MongoClient(config['mongo']['local'])
source_db = source_client['html_downloads']
source_collection = source_db['llm_feature_extract_dest']

# Destination database
dest_client = MongoClient(config['mongo']['remote'])
dest_db = dest_client['html_downloads']
dest_collection = dest_db['llm_feature_extract']

batch_size = 10_000
skip = 0

while True:

    # max_id_in_dst = dest_collection.find_one(sort=[('_id', -1)], projection={'_id': 1})

    # if max_id_in_dst is not None:
    #     find_query = {'_id': {'$gt': max_id_in_dst['_id']}}
    # else:
    #     find_query = {}
    find_query = {}

    logger.info(f'Reading {batch_size} docs, skip={skip}')

    source_ids_batch = [
        d['_id']
        for d in
        source_collection.find(find_query, {"_id": 1}).sort('_id', 1).skip(skip).limit(batch_size)
    ]
    if not source_ids_batch:
        break

    min_id = min(source_ids_batch)
    max_id = max(source_ids_batch)

    dest_ids_batch = [
        d['_id']
        for d in
        dest_collection.find({'_id': {'$gte': min_id, '$lte': max_id}}, {'_id': 1})
    ]

    missing_ids = set(source_ids_batch) - set(dest_ids_batch)

    if missing_ids:
        logger.info(f'Writing {len(missing_ids)} missing docs, skip={skip}')
        missing_docs = source_collection.find({'_id': {'$in': list(missing_ids)}})
        dest_collection.insert_many(missing_docs, ordered=False)

    skip += batch_size

logger.info('Done')