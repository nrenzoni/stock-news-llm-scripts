import logging

import toml
from pymongo import MongoClient, UpdateOne

from embeddings.embedding_calc import *

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

config = toml.load('../config.toml')

client = MongoClient(config['mongo']['remote'])
db = client['html_downloads']
collection = db['llm_feature_extract']

batch_size = 1_000

embedding_client = GrpcEmbeddingClient(config['embedding_server']['host'])

while True:

    find_query = {'summary_embeddings': {'$exists': False}}

    logger.info(f'Reading {batch_size} docs')

    summary_docs = list(
        collection
        .find(find_query, {"summary": 1})
        .sort('_id', 1)
        .limit(batch_size)
    )

    if not summary_docs:
        break

    upsert_operations = []

    summary_part_only_list = list(d['summary'] for d in summary_docs)
    as_str_list = json.dumps(summary_part_only_list)
    embeddings_docs = embedding_client.calc_embeddings(summary_part_only_list)

    for embedding, orig_doc in zip(embeddings_docs, summary_docs):
        embedding: np.ndarray
        upsert_operations.append(
            UpdateOne(
                {'_id': orig_doc['_id']},
                {'$set': {'summary_embeddings': embedding.tolist()}},
            )
        )

    if upsert_operations:
        logger.info(f'Writing {len(summary_docs)} missing summary embeddings')

        collection.bulk_write(
            upsert_operations,
            ordered=False
        )

logger.info('Done')
