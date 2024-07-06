from contextlib import contextmanager

import psycopg
from elasticsearch import Elasticsearch

from etl import config
from etl.etl_logger import logger
from etl.etl_utils import backoff


class ETL:
    @contextmanager
    def create_conn_pg(self):
        with psycopg.connect(
            f'postgresql://{config.POSTGRES_USER}:{config.POSTGRES_PASSWORD}@{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DBNAME}'
        ) as conn:
            with conn.cursor() as cur:
                yield cur

    @contextmanager
    def create_conn_es(self):
        with Elasticsearch(f'http://{config.ELASTIC_HOST}:{config.ELASTIC_PORT}/') as es:
            yield es

    def pipeline_film_works(self):
        self.indexing_es(config.MOVIES_INDEX, 'etl/sqls/fetch_film_works.sql')
        logger.info('Successfully indexing film works')

    def pipeline_genres(self):
        self.indexing_es(config.GENRE_INDEX, 'etl/sqls/fetch_genres.sql')
        logger.info('Successfully indexing genres')

    def pipeline_persons(self):
        self.indexing_es(config.PERSONS_INDEX, 'etl/sqls/fetch_persons.sql')
        logger.info('Successfully indexing persons')

    @backoff(steps=3)
    def indexing_es(self, index: str, sql_path: str):
        with self.create_conn_pg() as pg_conn, self.create_conn_es() as es_conn:
            with open(sql_path) as f:
                sql = f.read()

            pg_conn.execute(sql)
            data = pg_conn.fetchmany(config.SIZE_CHUNK)

            while data:
                body = []
                for row in data:
                    body.extend(
                        (
                            {
                                'index': {
                                    '_index': index,
                                    '_id': row[0]['id'],
                                }
                            },
                            row[0],
                        )
                    )

                result = es_conn.bulk(body=body)
                assert result['errors'] is False, 'ElasticSearch bulk operation failed.'
                data = pg_conn.fetchmany(config.SIZE_CHUNK)

    def run(self):
        self.pipeline_film_works()
        self.pipeline_genres()
        self.pipeline_persons()
        logger.info('ETL process successfully finished')
