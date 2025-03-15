import json
import os
from datetime import datetime
from utils import setup_logger

logger = setup_logger("migration-state-util")


def save_migration_state(directory: str, state: dict):
    os.makedirs(directory, exist_ok=True)
    current_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{current_date}-migrate-{state['batch_index']}-data.json"
    file_path = os.path.join(directory, file_name)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4)
    logger.info(f"Состояние миграции сохранено в файл: {file_path}")


def load_migration_state(directory: str, batch_index: int) -> dict:
    os.makedirs(directory, exist_ok=True)
    current_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"{current_date}-migrate-{batch_index}-data.json"
    file_path = os.path.join(directory, file_name)

    if not os.path.exists(file_path):
        return {"batch_index": batch_index, "offset": 0, "first_id": None, "last_id": None, "last_copied_id": None}

    with open(file_path, "r", encoding="utf-8") as f:
        state = json.load(f)
    logger.info(f"Состояние миграции загружено из файла: {file_path}")
    return state


def find_last_batch(directory: str) -> int:
    os.makedirs(directory, exist_ok=True)
    files = os.listdir(directory)
    batch_indices = []

    for file in files:
        if file.endswith("-data.json"):
            try:
                batch_index = int(file.split("-migrate-")[1].split("-data.json")[0])
                batch_indices.append(batch_index)
            except (ValueError, IndexError):
                continue

    return max(batch_indices) if batch_indices else 0
