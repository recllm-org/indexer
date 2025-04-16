from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, Boolean
from pgvector.sqlalchemy import Vector
from .utils import get_envars



envars = get_envars()
embedding_dim = int(envars.get('EMBEDDING_DIM'))


class Base(DeclarativeBase): pass


class RecLLMUsers(Base):
  __tablename__ = 'recllm_users'
  __table_args__ = {'extend_existing': True}
  id = mapped_column(Integer, primary_key=True, autoincrement=True)
  tablename = mapped_column(String)
  row_id = mapped_column(Integer)
  embedding = mapped_column(Vector(embedding_dim))
  context = mapped_column(String)
  stale = mapped_column(Boolean, default=False)


class RecLLMItems(Base):
  __tablename__ = 'recllm_items'
  __table_args__ = {'extend_existing': True}
  id = mapped_column(Integer, primary_key=True, autoincrement=True)
  tablename = mapped_column(String)
  row_id = mapped_column(Integer)
  embedding = mapped_column(Vector(embedding_dim))
  context = mapped_column(String)
  stale = mapped_column(Boolean, default=False)


class Table:
  def __init__(self, tablename, classname, tracked_columns=None, functions=None):
    self.tablename = tablename
    self.classname = classname
    self.tracked_columns = tracked_columns or []
    self.functions = functions or []
  
  def execute_functions(self, rows):
    for function in self.functions:
      function.execute(rows)
  
  def push(self, rows, session):
    raise NotImplementedError('push must be implemented!')


class UserTable(Table):
  def __init__(self, tablename, classname, tracked_columns=None, functions=None):
    super().__init__(tablename, classname, tracked_columns, functions)
    self.recllm_tablename = RecLLMUsers.__tablename__
  
  def push(self, rows, session):
    recllm_rows = []
    for row in rows:
      row.unlock()
      recllm_row = RecLLMUsers(
        tablename=self.tablename,
        row_id=row.id,
        embedding=row.cache.embedding,
        context=row.cache.context
      )
      recllm_rows.append(recllm_row)
    session.add_all(recllm_rows)


class ItemTable(Table):
  def __init__(self, tablename, classname, tracked_columns=None, functions=None):
    super().__init__(tablename, classname, tracked_columns, functions)
    self.recllm_tablename = RecLLMItems.__tablename__
  
  def push(self, rows, session):
    recllm_rows = []
    for row in rows:
      row.unlock()
      recllm_row = RecLLMItems(
        tablename=self.tablename,
        row_id=row.id,
        embedding=row.cache.embedding,
        context=row.cache.context
      )
      recllm_rows.append(recllm_row)
    session.add_all(recllm_rows)