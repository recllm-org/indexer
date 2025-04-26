from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, Boolean
from pgvector.sqlalchemy import Vector
from .record import Record
from .utils import EnvVars



class RecLLMBase(DeclarativeBase): pass


class RecLLMSATable(RecLLMBase):
  __abstract__ = True
  __table_args__ = {'extend_existing': True}
  id = mapped_column(Integer, primary_key=True, autoincrement=True)
  row_id = mapped_column(Integer)
  tablename = mapped_column(String)
  embedding = mapped_column(Vector(int(EnvVars.get('EMBEDDING_DIM'))))
  context = mapped_column(String)
  stale = mapped_column(Boolean, default=False)


class Table:
  def __init__(self, SATable, RecLLMSATable, tracked_columns=None, functions=None):
    self.SATable = SATable
    self.RecLLMSATable = RecLLMSATable
    self.tracked_columns = tracked_columns or []
    self.functions = functions or []
  
  def execute_functions(self, records):
    for function in self.functions:
      function.execute(records)
  
  def push(self, records, session):
    if not issubclass(self.RecLLMSATable, RecLLMSATable):
      raise NotImplementedError('push must be implemented for custom RecLLMSATable!')
    recllm_rows = []
    for record in records:
      record.unlock()
      row = record.get_row()
      recllm_row = self.RecLLMSATable(
        tablename=self.SATable.__table__.name,
        row_id=row.id,
        embedding=record.cache.embedding,
        context=record.cache.context
      )
      recllm_rows.append(recllm_row)
    session.add_all(recllm_rows)
  
  def update_stales(self, records, recllm_records):
    if not issubclass(self.RecLLMSATable, RecLLMSATable):
      raise NotImplementedError('update_stales must be implemented for custom RecLLMSATable!')
    for record, recllm_record in zip(records, recllm_records):
      recllm_record.unlock()
      recllm_row = recllm_record.get_row()
      recllm_row.embedding = record.cache.embedding
      recllm_row.context = record.cache.context
      recllm_row.stale = False

  def retrieve_stales(self, session, batch_size):
    if not issubclass(self.RecLLMSATable, RecLLMSATable):
      raise NotImplementedError('retrieve_stales must be implemented for custom RecLLMSATable!')
    batched_records = []
    batched_recllm_records = []
    offset = 0
    while True:
      recllm_rows = session\
        .query(self.RecLLMSATable)\
        .filter(self.RecLLMSATable.stale==True and self.RecLLMSATable.tablename==self.SATable.__tablename__)\
        .offset(offset)\
        .limit(batch_size)\
        .all()
      row_ids = [recllm_row.row_id for recllm_row in recllm_rows]
      rows = session.query(self.SATable).filter(self.SATable.id.in_(row_ids)).all()
      records = [Record(row, self) for row in rows]
      recllm_records = [Record(recllm_row, self) for recllm_row in recllm_rows]
      batched_records.append(records)
      batched_recllm_records.append(recllm_records)
      offset+=batch_size
      if len(recllm_records)<batch_size:
        break
    return batched_records, batched_recllm_records