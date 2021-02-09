# Monitor POC
Monitoring tool

Goals:
- [x] Have an API server that will periodically run jobs to check/ping external servers.
- [x] Have a fake server that will return predicted results
- [x] API should allow to start/cancel jobs and return it's status
- [ ] Have good tests

Current implementation doesn't store to database every message that was pushed to kafka.
It's store only metric values.

For example we have 2 metrics with granularity "minute" and "hour", this what we will see database: 

job_id | dt | granularity | rt_min | rt_max | data                              
-------|----|-------------|-------------------|-------------------|------------------------------------------------
1|2021-02-09 03:00:00.000000|hour|0.0044|0.0058|{"200": 2470, "404": 679, "500": 351}
1|2021-02-09 03:01:00.000000|minute|0.0043|0.0055|{"200": 35, "404": 18, "500": 5}
1|2021-02-09 03:02:00.000000|minute|0.00423|0.0052|{"200": 38, "404": 14, "500": 6}

- **dt** - is the UTC datetime
- **granularity** - is the period of time we aggregated data for
- **rt_min** and **rt_min** - min and max values for response time
- **data** - is JSONB structure to keep status_code counters. Because there could be a whole range of different HTTP 
status codes, it wasn't practical to keep them as columns. When this field is being updated - value for every key is 
being increased not overridden, this way we can keep correct stats.

## Deployment
Application is ready to be deployed to *Google App Engine*, but there's few steps should be taken to configure it.
1. Fill all variables in [settings/.env.example](settings/.env.example)
2. Copy "ca.pem", "service.cert", "service.key" to "settings" folder to have a secure SSH connection to Kafka

After that application can be deployed to *Google App Engine* with command:
* `gcloud app deploy`
  
## REST API
Implemented in [api.py](api.py)

Features:
- Configuration for all endpoints done in one file.
- Responsible for running ***Scheduler*** and optionally ***Message Processor***.

Endpoints:
- "/" - show basic information
- "/db/init" - reset database tables (DROP/CREATE)
- "/job" - show all existing monitoring jobs
- "/job/{job_id}" - show information for specific job with {job_id}
- "/job/{job_id}/start" - start job execution if it's not running
- "/job/{job_id}/stop" - stop job execution if it's running
- "/website_mock" - this end point will return random status_code and page content, used for testing.

## Scheduler
Implemented in [scheduler.py](scheduler.py)

Features:
- Load *Monitoring Job Configuration* from json file.
- Start Jobs
- Stop Jobs

Example of *Monitoring Job Configuration* can be found in jobs.json 

#####ToDo:
- Store information about *Monitoring Job Configuration* in database instead of json file.

## Monitor (Kafka Producer)
Implemented in module [monitors](monitors)

Base class ***BaseMonitor*** provide functionality to send messages to kafka and 
keeping stats about it's execution.
- Using ConfluentKafka library under the hood.

Class ***HttpMonitor*** override method "check" and do actual check of the HTTP endpoint.

HttpMonitor can be configured with next parameters:
- URI to check
- Method to use on specified URI. HEAD/GET/POST etc. Not all end points may support all methods,
that's why it is a configurable parameter.

## Processor (Kafka Consumer)
Implemented in module [processors](processors) 

Consume messages from Kafka, aggregate data from messages based on provided metrics, write data to 
database at predefined periods.

For example, it can consume 10k messages every second, and writing data to database every 10 seconds. 
So instead of making 100k request to database it will make a few - based on data it aggregated during 
this period.

Base class ***BaseMsgProcessor*** provide functionality to:
- Process incoming messages and extract/aggregate databased on metrics. 
- Upsert records to PostgreSQL at predefined periods.
- Keep aggregated data between calls to dump data to database.

2 classes that implement kafka connection using different libraries:
- ConfluentKafkaMsgProcessor - using Confluet Kafka library, it's a fastest python library but it's blocking.
- AioKafkaMsgProcessor - Using AioKafka library, slightly slower, but it's asynchronous.

#####ToDo:
- Function to upsert data to database is really crappy, need to fix it.
 
# Possible improvements
- ***Scheduler*** can be replaced with *task scheduling system* such as Celery, but then ***BaseMonitor*** 
should be changed to keep it's stats in database instead of memory, good option for database would be ***Redis***.
- Extend ***Processor*** metrics. Right now we can specify only ***granularity*** as metric parameter, but it should 
be possible to specify ***aggregate_function*** (min/max/avg), ***field*** - message field that will be used for 
aggravation.
- Only HTTP Monitoring Jobs are supported. It's possible to extend BaseMonitor to support 
other types of monitors, for example do SOCKS/WebSocket/Telnet/SSH monitors.
- There is a tight integration between ***Monitor*** and ***Processor***. ***Processor*** can 
process only specific types of messages. Need to come up with generic message format.  
