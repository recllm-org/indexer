from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, Boolean
from pgvector.sqlalchemy import Vector
from .utils import EnvVars



class Base(DeclarativeBase): pass


class RecLLMTable(Base):
  __abstract__ = True
  __table_args__ = {'extend_existing': True}
  id = mapped_column(Integer, primary_key=True, autoincrement=True)
  tablename = mapped_column(String)
  row_id = mapped_column(Integer)
  embedding = mapped_column(Vector(int(EnvVars.get('EMBEDDING_DIM'))))
  context = mapped_column(String)
  stale = mapped_column(Boolean, default=False)


class RecLLMUsers(RecLLMTable):
  __tablename__ = 'recllm_users'


class RecLLMItems(RecLLMTable):
  __tablename__ = 'recllm_items'


class Table:
  DEFAULT_TABLES = [RecLLMUsers, RecLLMItems]

  def __init__(self, tablename, classname, tracked_columns, functions, RecLLMTable):
    self.tablename = tablename
    self.classname = classname
    self.tracked_columns = tracked_columns or []
    self.functions = functions or []
    self.RecLLMTable = RecLLMTable
  
  def execute_functions(self, rows):
    for function in self.functions:
      function.execute(rows)
  
  def push(self, rows, session):
    if self.RecLLMTable not in Table.DEFAULT_TABLES:
      raise NotImplementedError('push must be implemented for custom RecLLMTable!')
    recllm_rows = []
    for row in rows:
      row.unlock()
      recllm_row = self.RecLLMTable(
        tablename=self.tablename,
        row_id=row.id,
        embedding=row.cache.embedding,
        context=row.cache.context
      )
      recllm_rows.append(recllm_row)
    session.add_all(recllm_rows)


class UserTable(Table):
  def __init__(self, tablename, classname, tracked_columns=None, functions=None, RecLLMTable=RecLLMUsers):
    super().__init__(tablename, classname, tracked_columns, functions, RecLLMTable)


class ItemTable(Table):
  def __init__(self, tablename, classname, tracked_columns=None, functions=None, RecLLMTable=RecLLMItems):
    super().__init__(tablename, classname, tracked_columns, functions, RecLLMTable)