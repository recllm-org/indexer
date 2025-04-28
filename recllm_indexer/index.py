"""
Indexes records into the database
Updates stale `Table` records in the database
"""



from .table import RecLLMBase



class Indexer:
  """
  `index`
    - Executes functions on records
    - Pushes records to the database
  
  `update_stales`
    - Retrieves stale `Table` records from the database in batches
    - For each batch
      - Executes functions on all stale records
      - Updates stale records in the database
  """

  def __init__(self, Table, db, Base=RecLLMBase):
    self.Table = Table
    self.db = db
    db.create_table(Table, Base)

  def index(self, records):
    with self.db.Session() as session:
      self.Table.execute_functions(records)
      self.Table.push(records, session)
      session.commit()
  
  def update_stales(self, batch_size=25):
    with self.db.Session() as session:
      batched_records, batched_recllm_records = self.Table.retrieve_stales(session, batch_size)
      for records, recllm_records in zip(batched_records, batched_recllm_records):
        self.Table.execute_functions(records)
        self.Table.update_stales(records, recllm_records)
      session.commit()