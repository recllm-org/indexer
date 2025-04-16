from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, Boolean
from pgvector.sqlalchemy import Vector


class Base(DeclarativeBase): pass


class RecLLMUsers(Base):
  __tablename__ = 'recllm_users'
  __table_args__ = {'extend_existing': True}
  id = mapped_column(Integer, primary_key=True, autoincrement=True)
  tablename = mapped_column(String)
  row_id = mapped_column(Integer)
  embedding = mapped_column(Vector(self.config.embedding_dim))
  context = mapped_column(String)
  stale = mapped_column(Boolean, default=False)


class RecLLMItems(Base):
  __tablename__ = 'recllm_items'
  __table_args__ = {'extend_existing': True}
  id = mapped_column(Integer, primary_key=True, autoincrement=True)
  tablename = mapped_column(String)
  row_id = mapped_column(Integer)
  embedding = mapped_column(Vector(self.config.embedding_dim))
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


class UserTable(Table):
  def __init__(self, tablename, classname, tracked_columns=None, functions=None):
    super().__init__(tablename, classname, tracked_columns, functions)
    self.recllm_tablename = RecLLMUsers.__tablename__


class ItemTable(Table):
  def __init__(self, tablename, classname, tracked_columns=None, functions=None):
    super().__init__(tablename, classname, tracked_columns, functions)
    self.recllm_tablename = RecLLMItems.__tablename__