import abc
import asyncio
import dataclasses
import logging
import time

import instructor
import google.generativeai as genai
import trafilatura
from instructor import AsyncInstructor

from feature_extractor.field_structure_definitions import FinancialNewsExtractedData
from feature_extractor.llm_providers import ILlmProvider, LlmWrapper

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class FinancialNewsExtractResult:
    data: FinancialNewsExtractedData
    model_name: str


class IFinancialNewsDataExtractor(abc.ABC):
    async def extract_async(self, html_content: str) -> FinancialNewsExtractResult:
        pass


class GeminiFinancialNewsDataExtractor(IFinancialNewsDataExtractor):
    def __init__(self, llm_provider: ILlmProvider):
        self.llm_provider = llm_provider

    async def extract_async(self, html_content: str) -> FinancialNewsExtractResult:
        # trafilatura.utils.check_html_lang(html_content)
        formated_content = trafilatura.extract(
            html_content,
            favor_recall=True,
            include_links=True
        )
        if formated_content is None:
            text = html_content
        else:
            text = formated_content

        while True:
            llm_wrapper = self.llm_provider.provide_llm()
            start_time = time.time()
            try:
                financial_news_extracted_data = await llm_wrapper.model.chat.completions.create(
                    messages=[
                        {
                            "role": "user",
                            "content": f"""
                            you are a financial news extractor expert, extract from the following article:
                            {text}""",
                        },
                    ],
                    max_retries=3,
                    response_model=FinancialNewsExtractedData,
                )

                logger.info(
                    f"Extracted using {llm_wrapper.model_name} LLM in {round(time.time() - start_time)} seconds")

                return FinancialNewsExtractResult(
                    financial_news_extracted_data,
                    llm_wrapper.model_name,
                )
            except Exception as e:
                logger.info(f"Error extracting from LLM: {e}")
            logger.info(f"Sleeping for 5 seconds before retrying")
            await self.llm_provider.sleep_until_next_ready_async()
