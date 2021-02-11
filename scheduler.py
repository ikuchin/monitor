import asyncio
import json
from typing import Dict

import aioschedule as schedule
import pendulum

from monitors.base_monitor import MonitorBase
from monitors.http_monitor import HttpMonitor

from settings import settings_path


class Scheduler:
    def __init__(self):
        self.start_dt = pendulum.now()
        self.jobs: Dict[int, MonitorBase] = {}
        self.running_jobs = {}

        self.load_jobs()

    def load_jobs(self) -> None:
        self.jobs = {}
        with open(f"{settings_path}/../jobs.json") as f:
            for job_params in json.load(f):
                self.add_job(self.create_job(**job_params))

    def add_job(self, job):
        """
        Adding Job to scheduler.jobs. Job is not started by default.

        :param job:
        :return:
        """

        self.jobs[job.job_id] = job
        return job

    def create_job(self, **kwargs):
        """
        Creating instance of the HttpMonitor and returning it.
        If params doesn't include job_id getting next available job_id.

        ToDo: at this point it can create only HttpMonitor jobs,
          need to extend this method to support different monitors.

        :param kwargs:
        :return:
        """
        if "job_id" not in kwargs:
            kwargs["job_id"] = max(self.jobs) + 1
        return HttpMonitor(**kwargs)  # ToDo: should use metaclass or factory

    def up_time(self) -> str:
        """
        Return Scheduler up time as human readable string.

        :return:
        """
        return (pendulum.now() - self.start_dt).in_words()

    async def start_scheduler(self) -> None:
        # If we want to start all jobs on scheduler start
        # for job_id in self.jobs:
        #     await self.job_start(job_id)

        while True:
            await schedule.run_pending()
            await asyncio.sleep(0.1)

    def job_start(self, job_id) -> None:
        """
        Start Job if it's not started. Will be calling Job "check" method every Job "check_period_seconds" seconds.

        :param job_id:
        :return:
        """
        if job_id in self.jobs and job_id not in self.running_jobs:
            job = self.jobs[job_id]
            self.running_jobs[job_id] = schedule.every(job.check_period_seconds).seconds.do(job.check)
            job.running = True

    def job_stop(self, job_id) -> None:
        """
        Stopping Job if it is running. Removing it from schedule.

        :param job_id:
        :return:
        """

        if job_id in self.running_jobs:
            job = self.running_jobs.pop(job_id)
            schedule.cancel_job(job)
            self.jobs[job_id].running = False
