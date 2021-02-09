import asyncio
import json
from typing import Dict

import aioschedule as schedule
import pendulum

from monitors.base_monitor import BaseMonitor
from monitors.http_monitor import HttpMonitor


class Scheduler:
    def __init__(self):
        self.start_dt = pendulum.now()
        self.jobs: Dict[int, BaseMonitor] = {}
        self.running_jobs = {}

        self.load_jobs()

    def load_jobs(self) -> None:
        with open("jobs.json") as f:
            self.jobs = {
                x["job_id"]: HttpMonitor(**x)  # ToDo: should use metaclass or factory
                for x in json.load(f)
            }

    def up_time(self) -> str:
        return (pendulum.now() - self.start_dt).in_words()

    async def start_scheduler(self) -> None:
        # If we want to start all jobs on scheduler start
        # for job_id in self.jobs:
        #     await self.job_start(job_id)

        while True:
            await schedule.run_pending()
            await asyncio.sleep(0.1)

    async def job_start(self, job_id) -> None:
        if job_id in self.jobs and job_id not in self.running_jobs:
            job = self.jobs[job_id]
            self.running_jobs[job_id] = schedule.every(job.check_period_seconds).seconds.do(job.check)
            job.running = True

    async def job_cancel(self, job_id) -> None:
        if job_id in self.running_jobs:
            job = self.running_jobs.pop(job_id)
            schedule.cancel_job(job)
            self.jobs[job_id].running = False
