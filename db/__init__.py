"""
Simple ORM for PostgreSQL
"""

import atexit
import logging

import psycopg2
from psycopg2 import sql

from settings import db_host, db_port, db_user, db_pass, db_name
from db.sql_statments import create_table_jobs_statement, create_table_stats_statement

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DB:
    # ToDo: This class imported in quite a few places, and for every instance it opening new connection to DB.
    #   Should turn this class to Singleton or keep only one connection per thread.
    def __init__(self):
        self._conn = None

        # self.create_tables()
        atexit.register(self.close_connection)

    def create_tables(self):
        self.execute_query(create_table_jobs_statement + create_table_stats_statement, auto_commit=True)

    @property
    def connection(self):
        if self._conn is None:
            print("Connecting", db_host, db_port, db_user, db_pass, db_name)
            self._conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_pass,
                dbname=db_name,
            )

        return self._conn

    def close_connection(self):
        if self._conn is not None:
            logger.info("Closing connection")
            self._conn.close()

    def create_db(self, db_name):
        query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
        return self.execute_query(query, auto_commit=True)

    def delete_db(self, db_name):
        query = sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name))
        return self.execute_query(query, auto_commit=True)

    def execute_query(self, query, params=None, auto_commit=True):
        if auto_commit:
            self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with self.connection.cursor() as cur:
            cur.execute(query, params)
            status_message = cur.statusmessage
            # records = None if cur.rowcount == -1 else cur.fetchall()
            records = None

        if auto_commit:
            self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_DEFAULT)

        return status_message, records
