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
  embedding = mapped_column(Vector(int(EnvVars.get('EMBEDDING_DIM'))))
  context = mapped_column(String)
  stale = mapped_column(Boolean, default=False)


class RecLLMUsers(RecLLMTable):
  __tablename__ = 'recllm_users'
  row_id = mapped_column(String)


class RecLLMItems(RecLLMTable):
  __tablename__ = 'recllm_items'
  row_id = mapped_column(Integer)


class Table:
  DEFAULT_TABLES = [RecLLMUsers, RecLLMItems]

  def __init__(self, tablename, classname, tracked_columns=None, functions=None, RecLLMTable=None):
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
  
  def update_stales(self, rows, recllm_rows):
    if self.RecLLMTable not in Table.DEFAULT_TABLES:
      raise NotImplementedError('update_stales must be implemented for custom RecLLMTable!')
    for row, recllm_row in zip(rows, recllm_rows):
      row.unlock()
      recllm_row.embedding = row.cache.embedding
      recllm_row.context = row.cache.context
      recllm_row.stale = False

  def retrieve_stales(self, table_class, session, batch_size):
    if self.RecLLMTable not in Table.DEFAULT_TABLES:
      raise NotImplementedError('retrieve_stales must be implemented for custom RecLLMTable!')
    batched_rows = []
    batched_recllm_rows = []
    offset = 0
    while True:
      recllm_rows = session.query(self.RecLLMTable).filter(self.RecLLMTable.stale==True and self.RecLLMTable.tablename==self.tablename).offset(offset).limit(batch_size).all()
      row_ids = [recllm_row.row_id for recllm_row in recllm_rows]
      rows = session.query(table_class).filter(table_class.id.in_(row_ids)).all()
      batched_rows.append(rows)
      batched_recllm_rows.append(recllm_rows)
      offset+=batch_size
      if len(recllm_rows)<batch_size:
        break
    return batched_rows, batched_recllm_rows


class UserTable(Table):
  def __init__(self, RecLLMTable=RecLLMUsers, *args, **kwargs):
    super().__init__(*args, **kwargs, RecLLMTable=RecLLMTable)


class ItemTable(Table):
  def __init__(self, RecLLMTable=RecLLMItems, *args, **kwargs):
    super().__init__(*args, **kwargs, RecLLMTable=RecLLMTable)