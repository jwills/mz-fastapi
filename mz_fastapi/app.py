from fastapi import FastAPI

from . import handlers, monitor

DSN = "postgresql://materialize@127.0.0.1:6875/materialize"

handler = handlers.PrintHandler()
query = monitor.Query(name="foo", sql="SELECT * FROM avg_bid WHERE avg > 200", handlers=[handler])
monitor = monitor.MaterializeQueryMonitor(dsn=DSN, queries=[query])

app = FastAPI()
monitor.expose(app)
