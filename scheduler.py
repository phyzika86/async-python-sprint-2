import logging

from collections import deque
from datetime import datetime

from job import Job
from exceptions import EarlyStartError
from dataclasses import dataclass
from typing import List
from my_tasks import get_tasks
import os
import pickle
from config import SAVED_TASK_DIR, SAVED_TASK_FILE
import keyboard

SAVED_TASK = SAVED_TASK_DIR + SAVED_TASK_FILE


@dataclass
class Scheduler:
    queue: deque
    completed_job_list: List
    is_running: bool
    not_completed_jobs: List
    pool_size: int

    def __init__(self):
        self.queue = deque([])
        self.completed_job_list = []
        self.is_running = True
        self.not_completed_jobs = []
        self.pool_size = 100
        self._key_board = False

    def schedule(self, job: Job):
        """
        Добавляем задачу в очередь
        """
        if len(self.queue) < self.pool_size:
            self.queue.appendleft(job)
        else:
            logging.info(f"Задача не уместилась в пул задач {job.kwargs.get('uuid')}")

    def shutdown(self):
        self._key_board = True

    def run(self):
        """
        Задачи разделены на два списка, выполненные и завершенные.
        """
        while True:
            try:
                if self._key_board:
                    raise KeyboardInterrupt

                keyboard.add_hotkey("ctrl+alt+j", self.shutdown)

                if job := self.queue.pop():
                    gen = job.generator
                    job_uuid = job.kwargs.get('uuid')

                    if not gen:
                        if not self.is_running:
                            self.not_completed_jobs.append(job)
                            continue

                        if job.dependencies and not self._check_all_dep_complete(job):
                            continue

                        gen = job.run()
                        job.generator = gen

                    self._check_working_time(job)
                    next(gen)

                    self.schedule(job)
            except StopIteration:
                self.completed_job_list.append(job_uuid)
            except EarlyStartError:
                self.schedule(job)
            except KeyboardInterrupt:
                self._key_board = False
                logging.info(
                    f"Планировщик остановлен пользователем. Невыполненные задачи будут выполнены при следующем запуске")
                self.stop()
            except FileNotFoundError as e:
                logging.error(
                    f"Дирректория или файл {e.filename} не существует либо еще не создан, осталось попыток перезапуска {job.retries}")
                if job.retries > 0:
                    logging.info(f"Перезапускаю job {job_uuid}")
                    job.decrease_tries()
                    self.schedule(job)
            except IndexError:
                break
            except Exception as e:
                logging.error(f"job {job_uuid} завершилась с ошибкой {e}, осталось попыток перезапуска {job.retries}")
                if job.retries > 0:
                    logging.info(f"Перезапускаю job {job_uuid}")
                    job.decrease_tries()
                    self.schedule(job)

    def get_queue(self):
        logging.info(f"Наполняю очередь задачами")
        TASKS = get_tasks()
        for task in TASKS:
            get_property_task = task.get
            job = Job(
                target=get_property_task('target'),
                kwargs=get_property_task('kwargs', {}),
                retries=get_property_task('retries', 0),
                dependencies=get_property_task('dependencies', []),
                start_at=get_property_task('start_at', datetime.now()),
            )
            self.schedule(job)

    def recovery_queue(self):
        logging.info(f"Восстанавливаю очередь задач после штатного завершения")
        with open(SAVED_TASK, 'rb') as f:
            while True:
                try:
                    data = pickle.load(f)
                    self.schedule(job=data)
                except EOFError:
                    break
        os.remove(SAVED_TASK) if os.path.exists(SAVED_TASK) else None

    def stop(self):
        self.is_running = False
        self.run()
        self._save_tasks(SAVED_TASK, self.not_completed_jobs)

    @staticmethod
    def _save_tasks(filename, jobs: list[Job]):
        with open(filename, 'wb') as f:
            for job in jobs:
                logging.info(f"Сохраняю задачу {job.kwargs['uuid']}")
                pickle.dump(job, f, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def _check_working_time(job: Job):
        """
        Метод получает время, прошедшее с момента запуска задачи и если время превысило допустимое, то отменяет задачу
        :param job:
        :return:
        """
        working_time = (datetime.now() - job.started_at).total_seconds()
        if working_time > job.max_working_time:
            raise StopIteration

    def _check_all_dep_complete(self, job: Job) -> bool:
        """
        Метод проверки того, что все зависимости задачи выполнены успешно
        """
        all_dep_complete = True
        for depend in job.dependencies:
            if depend not in self.completed_job_list:
                self.schedule(job)
                all_dep_complete = False
                break

        return all_dep_complete


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
    )

    scheduler = Scheduler()
    if not os.path.exists(SAVED_TASK):
        scheduler.get_queue()
        scheduler.run()
    else:
        scheduler.recovery_queue()
        scheduler.run()


if __name__ == '__main__':
    main()
