# monitor
POC - monitoring tool

Goals:
- Have an API server that will periodically run jobs to check/ping external servers.
- Have a fake server that will return predicted results
- API should allow to start/cancel jobs and return it's status
- Have good tests

Implementation:
- Jobs configurations should be stored in DB
- WE DO NEED SIMPLE ORM!
