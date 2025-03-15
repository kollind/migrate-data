import gc
from connectors import MSSQLConnector, PGSQLConnector
from io import StringIO
import csv
from utils import *

logger = setup_logger("migrate-data")


def mssql_connection_test(query):
    mssql_connector = MSSQLConnector()
    mssql_connector.connect()

    try:
        result = mssql_connector.select_query(query)
        if len(result) > 0:
            for row in result:
                print(row)
        else:
            logger.info("<<< Данных не найдено\n")
    finally:
        mssql_connector.disconnect()


def pgsql_connection_test(query):
    pgsql_connector = PGSQLConnector()
    pgsql_connector.connect()

    try:
        result = pgsql_connector.select_query(query)
        if len(result) > 0:
            for row in result:
                print(row)
        else:
            logger.info("<<< Данных не найдено\n")
    finally:
        pgsql_connector.disconnect()


def process_batch(mssql_connector,
                  postgres_connector,
                  batch_size: int,
                  offset: int,
                  batch_index: int,
                  state_directory: str):
    # TODO <-- вставить нужный скрипт для выборки данных.
    #  Подвал оставить для итеративного копирования. сolumn_name не забываем поменять
    select_from_mssql_query = f"""
                                order by column_name
                                offset {offset} rows
                                fetch next {batch_size} rows only;
                            """

    data = mssql_connector.select_query(select_from_mssql_query)
    if not data or data == []:
        logger.info("<<< Нет данных для обработки.")
        return False

    first_id = data[0][0]
    last_id = data[-1][0]
    logger.info(f"Начинается миграция данных: "
                f"Batch_Index={batch_index}, "
                f"Offset={offset}, "
                f"First_ID={first_id}, "
                f"Last_ID={last_id}")
    buffer = None

    try:
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(data)
        buffer.seek(0)

        postgres_connector.copy(buffer)
        logger.info(f"Пачка данных (OFFSET {offset}) успешно обработана.")

        state = {
            "batch_index": batch_index,
            "offset": offset + batch_size,
            "first_id": first_id,
            "last_id": last_id,
            "last_copied_id": last_id
        }
        save_migration_state(state_directory, state)
        return True
    except Exception as e:
        logger.error(f"<<< Ошибка при обработке данных: {e}")
    finally:
        buffer.close()
        del data
        gc.collect()


def main(batch_size):
    state_directory = "migration_states"
    last_batch_index = find_last_batch(state_directory)
    batch_index = last_batch_index + 1
    state = load_migration_state(state_directory, last_batch_index)
    offset = state.get("offset", 0)

    mssql_connector = MSSQLConnector()
    postgres_connector = PGSQLConnector()

    try:
        mssql_connector.connect()
        postgres_connector.connect()

        is_batch_get = True
        while is_batch_get:
            is_batch_get = process_batch(mssql_connector,
                                         postgres_connector,
                                         batch_size,
                                         offset,
                                         batch_index,
                                         state_directory)
            if is_batch_get:
                offset += batch_size
                batch_index += 1
    except Exception as e:
        logger.error(f"Миграция прервана: {e}")
    finally:
        mssql_connector.disconnect()
        postgres_connector.disconnect()
        logger.info("Миграция завершена")


if __name__ == "__main__":
    main(1)     # TODO <-- УКАЗАТЬ КАКИМИ ПАЧКАМИ ПЕРЕЛИВАТЬ ДАННЫЕ
