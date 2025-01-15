import abc
import datetime
from typing import Iterable, Any

from pymongo import MongoClient


class IRawHtmlReader(abc.ABC):

    def get_initial_skip_page(
            self, start_date: datetime.date, end_date_excl: datetime.date,
            dt_last_saved_url: datetime.datetime):
        raise NotImplementedError

    def read(self, start_date: datetime.date, end_date_excl: datetime.date, skip, limit) -> Iterable:
        raise NotImplementedError

    def read_all(self, skip, limit) -> Iterable:
        raise NotImplementedError

    @property
    def content_column_name(self) -> str:
        raise NotImplementedError

    @property
    def download_time_column_name(self) -> str:
        raise NotImplementedError


class ClickhouseRawHtmlReader(IRawHtmlReader):
    import clickhouse_connect

    def __init__(self, clickhouse_client: clickhouse_connect.driver.Client):
        self.clickhouse_client = clickhouse_client

    def get_initial_skip_page(self, start_date: datetime.date, end_date_excl: datetime.date,
                              dt_last_saved_url: datetime.datetime):
        query_res = self.clickhouse_client.query(
            f"""SELECT COUNT(*) FROM news.articles
                WHERE DownloadTime >= '{start_date.isoformat()}'
                  AND DownloadTime < '{dt_last_saved_url.isoformat()}'"""
        )

        total_count = query_res.result_rows[0][0] if query_res.result_rows else 0

        return total_count

    def read(self, start_date: datetime.date, end_date_excl: datetime.date, skip, limit) -> Iterable:
        """
        :return: arr of dict. each key has first letter in lowercase
        """
        query_res = self.clickhouse_client.query(
            f"""select * from news.articles
                where DownloadTime >= '{start_date.isoformat()}'
                  and DownloadTime < '{end_date_excl.isoformat()}'
                order by DownloadTime
                limit {limit} offset {skip}""")
        col_names = [n[0].lower() + n[1:] for n in query_res.column_names]
        for row in query_res.result_rows:
            yield dict(zip(col_names, row))

    def read_all(self, skip, limit) -> Iterable:
        query_res = self.clickhouse_client.query(
            f"""select * from news.articles
                order by DownloadTime
                limit {limit} offset {skip}""")
        col_names = [n[0].lower() + n[1:] for n in query_res.column_names]
        for row in query_res.result_rows:
            yield dict(zip(col_names, row))

    @property
    def content_column_name(self) -> str:
        return "htmlContent"

    @property
    def download_time_column_name(self) -> str:
        return "downloadTime"


class MongoRawHtmlReader(IRawHtmlReader):
    def __init__(self, mongo_client: MongoClient):
        self.mongo_client = mongo_client
        self.html_downloads_db = self.mongo_client["html_downloads"]
        self.html_raw_collection = self.html_downloads_db["html_raw"]
        self.dest_write_collection = self.html_downloads_db["llm_feature_extract_dest"]

    def read(self, start_date: datetime.date, end_date_excl: datetime.date, skip, limit) -> Iterable:
        start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        end_date_excl = datetime.datetime.combine(end_date_excl, datetime.datetime.min.time())

        pipeline = [
            {
                '$match': {
                    'url': {
                        '$regex': r'https:\/\/www\.globenewswire\.com\/news-release',
                        '$options': 'i'
                    }
                }
            },
            {
                '$project': {
                    'url': 1,
                    'dateString': {
                        '$regexFind': {
                            'input': '$url',
                            'regex': r'https:\/\/www\.globenewswire\.com\/news-release\/(2024\/\d{2}\/\d{2})\/'
                        }
                    },
                    'allFields': '$$ROOT'
                }
            },
            {
                '$project': {
                    'allFields': 1,
                    'dateString': {'$arrayElemAt': ['$dateString.captures', 0]}
                }
            },
            {
                '$project': {
                    'allFields': 1,
                    'date': {
                        '$dateFromString': {
                            'dateString': '$dateString',
                            'format': '%Y/%m/%d'
                        }
                    }
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': {
                        '$mergeObjects': ['$allFields', {'date': '$date'}]
                    }
                }
            },
            {
                '$match': {
                    'date': {
                        '$gte': start_date,
                        '$lt': end_date_excl
                    }
                }
            },
            {
                '$skip': skip
            },
            {
                '$limit': limit
            }

        ]

        return self.html_raw_collection.aggregate(pipeline)

    @property
    def content_column_name(self) -> str:
        return "content"

    @property
    def download_time_column_name(self) -> str:
        return "download_time"


class IFeatureResultRepo(abc.ABC):

    def get_dt_of_last_saved_url(self, start_date: datetime.date,
                                 end_date_excl: datetime.date) -> datetime.datetime | None:
        raise NotImplementedError

    def get_non_saved_urls(self, urls: list[str]) -> list[str]:
        raise NotImplementedError

    def write(self, docs_batch: list) -> None:
        raise NotImplementedError


class MongoFeatureResultRepo(IFeatureResultRepo):
    def __init__(self, mongo_client: MongoClient):
        self.mongo_client = mongo_client
        self.html_downloads_db = self.mongo_client["html_downloads"]
        self.html_raw_collection = self.html_downloads_db["html_raw"]
        self.dest_write_collection = self.html_downloads_db["llm_feature_extract_dest"]

    def get_dt_of_last_saved_url(self, start_date: datetime.date, end_date_excl: datetime.date):
        start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        end_date_excl = datetime.datetime.combine(end_date_excl, datetime.datetime.min.time())

        pipeline = [
            {
                '$match': {
                    'download_time': {
                        '$gte': start_date,
                        '$lt': end_date_excl
                    }
                }
            },
            {'$sort': {
                'download_time': -1
            }
            },
            {'$limit': 1},
            {
                '$project': {
                    '_id': 0,
                    'datetime': '$download_time',
                }
            }
        ]

        res = list(self.dest_write_collection.aggregate(pipeline))
        if not res:
            return None
        return res[0]['datetime']

    def get_non_saved_urls(self, urls: list[str]) -> list[str]:
        existing_urls = set(
            doc["url"]
            for doc in self.dest_write_collection.find({
                "url": {
                    "$in": urls
                }
            }))

        return [url for url in urls if url not in existing_urls]

    def write(self, docs_batch: list) -> None:
        processed = []
        for doc in docs_batch:
            processed.append(doc)
        self.dest_write_collection.insert_many(processed)
