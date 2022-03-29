
import asyncio
import logging
from dataclasses import dataclass
from typing import List

import psycopg
from fastapi import FastAPI

from . import handlers

logger = logging.getLogger(__name__)


@dataclass
class Query:
  name: str
  sql: str
  handlers: List[handlers.Handler]


async def monitor(dsn: str, q: Query):
  conn = await psycopg.AsyncConnection.connect(dsn)
  cursor = conn.cursor()

  logger.info("Monitoring query named: %s", q.name)
  tail_query = f"TAIL ({q.sql})"
  async for (timestamp, diff, *columns) in cursor.stream(tail_query):
    record = handlers.Record(query_name=q.name, timestamp=timestamp, diff=diff, columns=columns)
    for h in q.handlers:
      h.emit(record)


class MaterializeQueryMonitor:
  def __init__(self, dsn: str, queries: List[Query]):
    self.dsn = dsn
    self.queries = {q.name: q for q in queries}
    self.tasks = []

  def expose(self, app: FastAPI):

    @app.on_event("startup")
    def monitor_queries():
      loop = asyncio.get_running_loop()
      for q in self.queries.values():
        logger.info(f"Setting up monitoring for {q.name}")
        task = loop.create_task(monitor(self.dsn, q), name=q.name)
        self.tasks.append(task)

    @app.get("/queries")
    def list_queries():
      return [q for q in self.queries]

    @app.get("/queries/{query}")
    def get_query(query: str):
      q = self.queries.get(query)
      if q:
        return {"name": q.name, "sql": q.sql}
      else:
        return None
