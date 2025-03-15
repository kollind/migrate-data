import pyodbc
from dotenv import load_dotenv
import os
from utils import setup_logger


class MSSQLConnector:

    def __init__(self):
        load_dotenv()
        self.server = os.getenv("MSSQL_SERVER")
        self.database = os.getenv("MSSQL_DATABASE")
        self.username = os.getenv("MSSQL_USERNAME")
        self.password = os.getenv("MSSQL_PASSWORD")
        self.connection = None
        self.logger = setup_logger("mssql-connector")

    def connect(self):
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password}"
            )
            self.connection = pyodbc.connect(connection_string)
            self.logger.info("<<< Успешное подключение к MSSQL\n")
        except pyodbc.Error as e:
            self.logger.error(f"<<< Ошибка подключения к MSSQL: {e}")
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.logger.info("<<< Соединение с MSSQL закрыто.")

    def select_query(self, query: str):
        if not self.connection:
            raise Exception("<<< Соединение с MSSQL не установлено\n")

        try:
            cursor = self.connection.cursor()
            self.logger.info(f">>> Отправляем запрос")
            cursor.execute(query)
            result = cursor.fetchall()
            self.logger.info("<<< Запрос выполнен успешно\n")
            return result
        except pyodbc.Error as e:
            self.logger.error(f"Ошибка при выполнении запроса: {e}")
            self.connection.rollback()
            raise
        