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



def validate_table(func):
  def wrapper(cls, *args, **kwargs):
    assert cls.SATable is not None, 'SATable needs to be set!'
    assert cls.RecLLMSATable is not None, 'RecLLMSATable needs to be set!'
    if not issubclass(cls.RecLLMSATable, RecLLMSATable):
      raise NotImplementedError('RecLLMSATable needs to be a subclass of RecLLMSATable!')
    return func(cls, *args, **kwargs)
  return wrapper


class Table:
  SATable = None
  RecLLMSATable = None
  tracked_columns = []
  functions = []
  
  @classmethod 
  @validate_table
  def execute_functions(cls, records):
    for function in cls.functions:
      function.execute(records)
  
  @classmethod
  @validate_table
  def push(cls, records, session):
    recllm_rows = []
    for record in records:
      record.unlock()
      row = record.get_row()
      recllm_row = cls.RecLLMSATable(
        tablename=cls.SATable.__table__.name,
        row_id=row.id,
        embedding=record.cache.embedding,
        context=record.cache.context
      )
      recllm_rows.append(recllm_row)
    session.add_all(recllm_rows)
  
  @classmethod
  @validate_table
  def update_stales(cls, records, recllm_records):
    for record, recllm_record in zip(records, recllm_records):
      recllm_record.unlock()
      recllm_row = recllm_record.get_row()
      recllm_row.embedding = record.cache.embedding
      recllm_row.context = record.cache.context
      recllm_row.stale = False

  @classmethod
  @validate_table
  def retrieve_stales(cls, session, batch_size):
    batched_records = []
    batched_recllm_records = []
    offset = 0
    while True:
      recllm_rows = session\
        .query(cls.RecLLMSATable)\
        .filter(cls.RecLLMSATable.stale==True and cls.RecLLMSATable.tablename==cls.SATable.__tablename__)\
        .offset(offset)\
        .limit(batch_size)\
        .all()
      row_ids = [recllm_row.row_id for recllm_row in recllm_rows]
      rows = session.query(cls.SATable).filter(cls.SATable.id.in_(row_ids)).all()
      records = [Record(row, cls) for row in rows]
      recllm_records = [Record(recllm_row) for recllm_row in recllm_rows]
      batched_records.append(records)
      batched_recllm_records.append(recllm_records)
      offset+=batch_size
      if len(recllm_records)<batch_size:
        break
    return batched_records, batched_recllm_records