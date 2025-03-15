import psycopg2
from dotenv import load_dotenv
import os
from utils import setup_logger


class PGSQLConnector:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("PGSQL_HOST")
        self.database = os.getenv("PGSQL_DATABASE")
        self.username = os.getenv("PGSQL_USERNAME")
        self.password = os.getenv("PGSQL_PASSWORD")
        self.connection = None
        self.logger = setup_logger("pgsql_connector")

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=5433,      # TODO <-- добавлял для себя, убрать, если не нужен
                database=self.database,
                user=self.username,
                password=self.password
            )
            self.logger.info("<<< Успешное подключение к PostgreSQL\n")
        except psycopg2.Error as e:
            self.logger.error(f"<<< Подключение к PostgreSQL не удалось: {e}")
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.logger.info("<<< Соединение с PostgreSQL закрыто\n")

    def select_query(self, query: str):
        if not self.connection:
            self.logger.error("<<< Соединение с PostgreSQL не установлено\n")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except psycopg2.Error as e:
            self.logger.info(f"<<< Ошибка при выполнении запроса: {e}")
            self.connection.rollback()
            raise

    def copy(self, buffer):
        # TODO <-- Указать реальные schema_name.table_name
        query = """
                    COPY schema_name.table_name 
                    FROM STDIN WITH CSV
                """

        if not self.connection:
            raise Exception("<<< Соединение с PostgreSQL не установлено\n")

        try:
            cursor = self.connection.cursor()
            cursor.copy_expert(query, buffer)
            self.connection.commit()
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка загрузки батча: {e}")
            self.connection.rollback()
            raise
