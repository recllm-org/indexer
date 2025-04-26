from .db import Database



class Indexer:
  def __init__(self, tables):
    self.tables = tables
    self.db = Database(tables)
  
  def index(self, records):
    grouped_records = Indexer.group_records(records)
    with self.db.Session() as session:
      for table, records in grouped_records.items():
        table.execute_functions(records)
        table.push(records, session)
      session.commit()
  
  def update_stales(self, batch_size=25):
    with self.db.Session() as session:
      for table in self.tables:
        batched_records, batched_recllm_records = table.retrieve_stales(session, batch_size)
        for records, recllm_records in zip(batched_records, batched_recllm_records):
          table.execute_functions(records)
          table.update_stales(records, recllm_records)
      session.commit()

  @staticmethod
  def group_records(records):
    grouped_records = {}
    for record in records:
      if record.table not in grouped_records:
        grouped_records[record.table] = []
      grouped_records[record.table].append(record)
    return grouped_records