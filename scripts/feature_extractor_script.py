import asyncio
import datetime
import logging
import clickhouse_connect
import toml

from feature_extractor.llm_providers import ILlmProvider
from feature_extractor.raw_html_reading import MongoFeatureResultRepo, ClickhouseRawHtmlReader
from feature_extractor.extractor_pipelines import ExtractorPipeline
from feature_extractor.feature_extract import GeminiFinancialNewsDataExtractor
from pymongo import MongoClient

try:
    from proprietary_setup import llm_provider
except ImportError:
    llm_provider = ILlmProvider()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

config = toml.load('../config.toml')

# setup_env()

mongo_client = MongoClient(config['mongo']['local'])

config_clickhouse = config['clickhouse']
ch_client = clickhouse_connect.get_client(
    username=config_clickhouse['username'],
    password=config_clickhouse['password'],
    database=config_clickhouse['database']
)
ch_html_reader = ClickhouseRawHtmlReader(ch_client)

# mongo_raw_html_reader = MongoRawHtmlReader(mongo_client)

extractor_pipeline = ExtractorPipeline(
    raw_html_reader=ch_html_reader,
    feature_extractor=GeminiFinancialNewsDataExtractor(llm_provider),
    feature_result_repo=MongoFeatureResultRepo(mongo_client),
    # test_single_write=True
)

start_date = datetime.date(2024, 10, 22)
end_date = datetime.date(2024, 10, 25)

logger.info(f'running pipeline for dates [{start_date}, {end_date})')

asyncio.run(
    extractor_pipeline.run(
        start_date,
        end_date
    )
)
