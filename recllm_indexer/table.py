"""
`SATable` is a SQLAlchemy table
`RecLLMSATable` is a SQLAlchemy table that stores the embeddings and contexts of the `SATable` rows
`Table` is a wrapper that connects `SATable` with `RecLLMSATable` along with the required functions
"""



from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy import Integer, String, Boolean
from .record import Record



class RecLLMBase(DeclarativeBase): pass


class RecLLMSATable(RecLLMBase):
  __abstract__ = True
  __table_args__ = {'extend_existing': True}
  id = mapped_column(Integer, primary_key=True, autoincrement=True)
  row_id = mapped_column(Integer)
  tablename = mapped_column(String)
  context = mapped_column(String)
  stale = mapped_column(Boolean, default=False)

  def __init_subclass__(cls):
    if not hasattr(cls, 'embedding'):
      raise NotImplementedError(f'`embedding` needs to be set in {cls.__name__}!')
    if not hasattr(cls, '__tablename__'):
      raise NotImplementedError(f'`__tablename__` needs to be set in {cls.__name__}!')



def validate_table(func):
  """
  Validates that the `SATable` and `RecLLMSATable` are set and that `RecLLMSATable` is a subclass of `RecLLMBase`
  If not, raises an error
  """
  def wrapper(cls, *args, **kwargs):
    assert cls.SATable is not None, 'SATable needs to be set!'
    assert cls.RecLLMSATable is not None, 'RecLLMSATable needs to be set!'
    if not issubclass(cls.RecLLMSATable, RecLLMSATable):
      raise NotImplementedError('RecLLMSATable needs to be a subclass of RecLLMSATable!')
    return func(cls, *args, **kwargs)
  return wrapper


class Table:
  """
  Executes `Function`s on records
  Pushes records to the database
  Updates stale `RecLLMSATable` records
  Retrieves stale `RecLLMSATable` records
  """

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
    """
    Unlocks the records, gets their corresponding rows
    Creates `RecLLMSATable` rows with the embeddings and contexts of the records
    Pushes the `RecLLMSATable` rows to the database
    """
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
    """
    Unlocks the records, gets their corresponding rows
    Updates the `RecLLMSATable` rows with the updated embeddings and contexts of the records
    Sets the `RecLLMSATable` rows to stale=False
    """
    for record, recllm_record in zip(records, recllm_records):
      recllm_record.unlock()
      recllm_row = recllm_record.get_row()
      recllm_row.embedding = record.cache.embedding
      recllm_row.context = record.cache.context
      recllm_row.stale = False

  @classmethod
  @validate_table
  def retrieve_stales(cls, session, batch_size):
    """
    Retrieves stale `RecLLMSATable` records in batches
    First filters only rows that are stale and belong to the `SATable`
    Then, for each batch
      - Gets the row ids of the records
      - `offset` and `limit` are used to paginate through the rows
      - Gets the rows of the records
      - Creates records from the rows
    """
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