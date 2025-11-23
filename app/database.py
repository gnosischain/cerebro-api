import clickhouse_connect
from app.config import settings
from typing import Any, List, Dict

class ClickHouseClient:
    _client = None

    @classmethod
    def get_client(cls):
        """
        Returns a singleton ClickHouse client instance.
        """
        if cls._client is None:
            cls._client = clickhouse_connect.get_client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_PORT,
                username=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD,
                database=settings.CLICKHOUSE_DATABASE,
                secure=settings.CLICKHOUSE_SECURE
            )
        return cls._client

    @classmethod
    def query(cls, query_str: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        """
        Executes a parameterized query and returns a list of dictionaries.
        """
        client = cls.get_client()
        try:
            result = client.query(query_str, parameters=parameters)
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        except Exception as e:
            # In a real app, you might want to log this to Sentry/Datadog
            print(f"DB Error: {e}")
            raise e