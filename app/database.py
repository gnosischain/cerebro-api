import logging
from typing import Any, Dict, List

import clickhouse_connect

from app.config import settings

logger = logging.getLogger("cerebro_api.database")


class ClickHouseClient:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            cls._client = clickhouse_connect.get_client(
                host=settings.CLICKHOUSE_URL,
                port=settings.CLICKHOUSE_PORT,
                username=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD,
                database=settings.CLICKHOUSE_DATABASE,
                secure=settings.CLICKHOUSE_SECURE,
            )
        return cls._client

    @classmethod
    def query(cls, query_str: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        client = cls.get_client()
        try:
            result = client.query(query_str, parameters=parameters)
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        except Exception as e:
            logger.error("ClickHouse query error: %s", e)
            raise e
