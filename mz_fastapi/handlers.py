import abc

from dataclasses import dataclass
from typing import Any, List, Optional

@dataclass
class Record:
  query_name: str
  timestamp: int
  diff: Optional[int]
  columns: List[Any]


class Handler:
  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def emit(self, record: Record):
    """TBD"""
    pass


class NullHandler(Handler):
  def emit(self, record: Record):
    pass


class PrintHandler(Handler):
  def emit(self, record: Record):
    print(record)


class MemoryHandler(Handler):
  def __init__(self):
    self.records = []

  def emit(self, record: Record):
    self.records.append(record)    
