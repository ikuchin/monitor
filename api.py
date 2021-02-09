import asyncio

from fastapi import FastAPI, responses

from db import DB
from processors.msg_processor_aiokafka import AioKafkaMsgProcessor
from scheduler import Scheduler
from db.sql_statments import create_table_jobs_statement, create_table_stats_statement
from website import WebsiteMock

app = FastAPI()
db = DB()
scheduler = Scheduler()
website_mock = WebsiteMock()

# We can run Message Processor (Kafka message consumer) in the same process,
# this may affect performance in negative way on the hight load. But it's should be fine for POC
msg_processor = AioKafkaMsgProcessor(kafka_topics=["test"])


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scheduler.start_scheduler())
    if msg_processor:
        asyncio.create_task(msg_processor.loop())


@app.get("/")
def index():
    stats = {
        "scheduler_up_time": scheduler.up_time(),
        "number_of_running_jobs": len(scheduler.running_jobs),
        "status": "ok",
        "number_of_produced_messages": sum(job.number_of_send_messages for job in scheduler.jobs.values())
    }

    if msg_processor:
        stats["number_of_consumed_messages"] = msg_processor.number_of_received_messages

    return stats


@app.get("/db/init")
def db_init():
    """
    Create tables

    :return:
    """
    db.execute_query("DROP TABLE IF EXISTS jobs, stats;", auto_commit=True)
    db.execute_query(create_table_jobs_statement + create_table_stats_statement, auto_commit=True)

    return {"status": "ok"}


@app.get("/job", summary="List all Jobs", tags=["Jobs"])
def job_list():
    return [
        {
            job_id: job.job_name
        }
        for job_id, job in scheduler.jobs.items()
    ]


@app.get("/job/{job_id}", summary="Show Job info", tags=["Jobs"])
def job_status(job_id: int):
    job = scheduler.jobs.get(job_id)

    if job:
        return job.dict()
    else:
        return {"error": f"Job with {job_id=} not found"}


@app.post("/job", summary="Start Job", tags=["Jobs"])
def job_create():
    return {}


@app.get("/job/{job_id}/start", summary="Start Job", tags=["Jobs"])
async def job_start(job_id: int):
    await scheduler.job_start(job_id)
    return {"status": "ok"}


@app.get("/job/{job_id}/stop", summary="Cancel Job", tags=["Jobs"])
async def job_cancel(job_id: int):
    await scheduler.job_cancel(job_id)
    return {"status": "ok"}


@app.get("/website_mock", summary="")
def website():
    status_code, content = website_mock.get()

    return responses.HTMLResponse(content=content, status_code=status_code)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run("api:app", host="0.0.0.0", port=8080, reload=False)
