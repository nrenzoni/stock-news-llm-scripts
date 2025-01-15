import asyncio
import logging

import more_itertools

from feature_extractor.raw_html_reading import IRawHtmlReader, IFeatureResultRepo
from feature_extractor.feature_extract import IFinancialNewsDataExtractor, FinancialNewsExtractResult

logger = logging.getLogger(__name__)


class ExtractorPipeline:
    def __init__(
            self,
            raw_html_reader: IRawHtmlReader,
            feature_extractor: IFinancialNewsDataExtractor,
            feature_result_repo: IFeatureResultRepo,
            test_single_write: bool = False
    ):
        self.raw_html_reader = raw_html_reader
        self.feature_extractor = feature_extractor
        self.feature_result_repo = feature_result_repo
        self.test_single_write = test_single_write

    async def run(self, start_date, end_date_excl):

        limit = 100
        chunk_size = 5 if not self.test_single_write else 1

        dt_of_last_saved_url = self.feature_result_repo.get_dt_of_last_saved_url(start_date, end_date_excl)

        if dt_of_last_saved_url:
            skip = self.raw_html_reader.get_initial_skip_page(start_date, end_date_excl, dt_of_last_saved_url)
        else:
            skip = 0

        while True:
            docs = list(self.raw_html_reader.read(start_date, end_date_excl, skip, limit))
            if not docs:
                break
            all_urls = [doc["url"] for doc in docs]
            new_urls = self.feature_result_repo.get_non_saved_urls(all_urls)
            new_docs = [doc for doc in docs if doc["url"] in new_urls]

            for i, chunk in enumerate(more_itertools.chunked(new_docs, chunk_size)):
                feature_docs, failed = await self.extract_chunk(chunk, i)

                writeable_docs = [
                    self.build_writeable_doc(extracted_data, doc)
                    for extracted_data, doc
                    in zip(feature_docs, chunk)
                ]

                if len(writeable_docs) > 0:
                    self.feature_result_repo.write(writeable_docs)
                if self.test_single_write or failed:
                    break

            if self.test_single_write:
                break

            skip += limit

    async def extract_chunk(self, chunk, i):
        tasks = []
        for j, doc in enumerate(chunk):
            logger.info(f"Extracting features for doc {j + 1} chunk {i + 1}")
            tasks.append(self.feature_extractor.extract_async(doc[self.raw_html_reader.content_column_name]))

        feature_docs = []
        failed = False
        try:
            feature_docs = await asyncio.gather(*tasks)
        except Exception as e:
            logger.info(f"Error extracting features: {e}")
            failed = True

        return feature_docs, failed

    def build_writeable_doc(self, extract_result: FinancialNewsExtractResult, original_doc):
        extracted_data_as_dict = extract_result.data.model_dump()

        return {
            "url": original_doc["url"],
            "download_time": original_doc[self.raw_html_reader.download_time_column_name],
            "publish_time": original_doc["publishTime"],
            "article_title": original_doc["articleTitle"],
            # "content": original_doc["content"],
            **extracted_data_as_dict,
            "model_name": extract_result.model_name
        }
