import logging

import requests
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Generator
from utils import coroutine
from exceptions import EarlyStartError
import logging


@dataclass
class Job:
    """
    start_at -- время, когда задача должна стартовать
    max_working_time -- Длительность выполнения задачи
    tries -- количество рестартов
    dependencies -- зависимости задачи, от которых зависит ее выполнение
    started_at -- фактическое время старта
    """
    target: Callable
    kwargs: dict
    generator: Generator | None = None
    retries: int = 0
    max_working_time: int = 2
    started_at: datetime | None = None
    dependencies: list[str] | None = None
    start_at: datetime = datetime.now()

    @coroutine
    def run(self):
        if self.start_at > (fact_at := datetime.now()):
            raise EarlyStartError(
                f'Попытка запустить задачу в {fact_at} раньше времени ожидаемого старата: {self.start_at}')

        self.started_at = datetime.now()
        logging.info(f"Задача {self.kwargs['uuid']} запущена. Выполняю {self.target.__name__}")
        yield from self.target(**self.kwargs)

    def decrease_tries(self):
        logging.info(f"Уменьшаю количество попыток перезапуска для задачи {self.kwargs['uuid']}")
        self.retries -= 1


if __name__ == '__main__':
    r = requests.get('https://www.cbr-xml-daily.ru/daily_json.js')
    res = json.loads(r.text)
    pass
