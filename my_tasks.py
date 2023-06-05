import json
from datetime import datetime, timedelta
import uuid
import os
import shutil
from typing import Generator
import logging
from utils import coroutine
import requests
import pickle


@coroutine
def print_log() -> Generator:
    # будим использовать данную корутину для вывода логов
    # лог можно отправить в корутину методом send
    while True:
        message = yield
        logging.info(message)


def create_dir(**kwargs) -> Generator:
    print_log().send(f"Начинаю создание дирректории tmp_{kwargs['name_dir']}")
    yield
    os.makedirs(f"tmp_{kwargs['name_dir']}", exist_ok=True)
    print_log().send(f"Создание дирректории tmp_{kwargs['name_dir']} завершено")


def delete_dir(**kwargs) -> Generator:
    print_log().send(f"Начинаю удаление дирректории tmp_{kwargs['name_dir']}")
    yield
    shutil.rmtree(f"tmp_{kwargs['name_dir']}/", ignore_errors=True)
    print_log().send(f"Удаление дирректории tmp_{kwargs['name_dir']}")


def create_file(**kwargs) -> Generator:
    print_log().send("Начинаю процесс создания файла")
    yield
    print_log().send("Выполняю запрос")
    response = requests.get(f"{kwargs['url']}")
    data = json.loads(response.text)
    yield
    with open(f"tmp/{kwargs['name_file']}.txt", 'w') as f:
        print_log().send(f"Начинаюю запись в файл tmp/{kwargs['name_file']}")
        for valuate in data['Valute'].items():
            f.write(f"{valuate[0]}: {json.dumps(valuate[1])}\n")
            yield
            print_log().send(f"Продолжаю запись в файл tmp/{kwargs['name_file']}")
    print_log().send(f"Запись в файл tmp/{kwargs['name_file']} окончена")


def delete_file(**kwargs) -> Generator:
    print_log().send(f"Начинаю удаление файла {kwargs['name_file']}")
    yield
    os.remove(f"tmp/{kwargs['name_file']}/")
    print_log().send(f"Удаление файла {kwargs['name_file']} завершено")


def create_file_with_error(**kwargs) -> Generator:
    with open(f"test_tmp_error/{kwargs['name_file']}.txt", 'w', encoding='utf8') as f:
        print_log().send(f"Начинаюю запись в файл test_tmp_error/{kwargs['name_file']}")
        f.write(f"Если видишь это сообщение значит запись с проверкой перезапуска завершена")
        yield
        print_log().send(f"Запись в файл test_tmp_error/{kwargs['name_file']} окончена")


def create_dir_test_tmp_error(**kwargs) -> Generator:
    shutil.rmtree(f"test_tmp_error/", ignore_errors=True)
    yield
    os.makedirs("test_tmp_error", exist_ok=True)


def get_tasks():
    UUIDS = {f'uuid_{i}': uuid.uuid4() for i in range(20)}

    TASKS = [
        {
            'target': create_dir,
            'retries': 100000,
            'kwargs': {
                'uuid': UUIDS.get('uuid_1'),
                'name_dir': UUIDS.get('uuid_1')
            }
        },
        {
            'target': create_dir,
            'kwargs': {
                'uuid': UUIDS.get('uuid_2'),
                'name_dir': UUIDS.get('uuid_2')
            }
        },
        {
            'target': create_dir,
            'kwargs': {
                'uuid': UUIDS.get('uuid_3'),
                'name_dir': UUIDS.get('uuid_3')
            }
        },
        {
            'target': delete_dir,
            'dependencies': [UUIDS.get('uuid_2')],
            'kwargs': {
                'uuid': UUIDS.get('uuid_4'),
                'name_dir': UUIDS.get('uuid_2')
            }
        },
        {
            'target': create_file,
            'retries': 2,
            'kwargs': {
                'uuid': UUIDS.get('uuid_5'),
                'name_file': UUIDS.get('uuid_5'),
                'url': 'https://www.cbr-xml-daily.ru/daily_json.js'
            }
        },
        {
            'target': create_file,
            'retries': 2,
            'kwargs': {
                'uuid': UUIDS.get('uuid_6'),
                'name_file': UUIDS.get('uuid_6'),
                'url': 'https://www.cbr-xml-daily.ru/daily_json.js'
            }
        },
        {
            'target': create_file,
            'retries': 10000,
            'kwargs': {
                'uuid': UUIDS.get('uuid_7'),
                'name_file': UUIDS.get('uuid_7'),
                'url': 'https://www.cbr-xml-daily.ru/daily_json.js'
            }
        },
        {
            'target': create_file_with_error,
            'retries': 10000,
            'kwargs': {
                'uuid': UUIDS.get('uuid_8'),
                'name_file': UUIDS.get('uuid_8'),
            }
        },
        {
            'target': create_dir_test_tmp_error,
            'retries': 10,
            'kwargs': {
                'uuid': UUIDS.get('uuid_9'),
                'name_file': UUIDS.get('uuid_9'),
            }
        },
        {
            'target': create_dir,
            'start_at': datetime.now() + timedelta(seconds=1),
            'retries': 10,
            'kwargs': {
                'uuid': UUIDS.get('uuid_10'),
                'name_dir': UUIDS.get('uuid_10'),
            }
        },
        {
            'target': delete_file,
            'start_at': datetime.now() + timedelta(seconds=1),
            'retries': 10,
            'kwargs': {
                'uuid': UUIDS.get('uuid_11'),
                'name_file': UUIDS.get('uuid_7'),
            }
        },
    ]

    shutil.rmtree(f"test/tasks.txt", ignore_errors=True)
    with open(f"test/tasks.txt", 'wb') as f:
        for task in TASKS:
            pickle.dump(task, f, protocol=pickle.HIGHEST_PROTOCOL)

    return TASKS
