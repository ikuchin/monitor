create_table_jobs_statement = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id serial PRIMARY KEY,
    job_name VARCHAR ( 255 ),
    created_on TIMESTAMP NOT NULL,
    uri VARCHAR ( 255 ) NOT NULL,
    check_period INT,
    check_cron VARCHAR ( 32 )
);
"""

create_table_stats_statement = """
CREATE TABLE IF NOT EXISTS stats (
    job_id INT NOT NULL,
    ts TIMESTAMP NOT NULL ,
    granularity VARCHAR ( 32 ) NOT NULL ,
    response_time_min FLOAT,
    response_time_max FLOAT,
    data JSONB NOT NULL DEFAULT '{}',
    UNIQUE (job_id, ts, granularity)
);
"""