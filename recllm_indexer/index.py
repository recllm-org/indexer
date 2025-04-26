from .db import Database



class Indexer:
  def __init__(self, Tables):
    self.Tables = Tables
    self.db = Database(Tables)
  
  def index(self, records):
    grouped_records = Indexer.group_records(records)
    with self.db.Session() as session:
      for Table, records in grouped_records.items():
        Table.execute_functions(records)
        Table.push(records, session)
      session.commit()
  
  def update_stales(self, batch_size=25):
    with self.db.Session() as session:
      for Table in self.Tables:
        batched_records, batched_recllm_records = Table.retrieve_stales(session, batch_size)
        for records, recllm_records in zip(batched_records, batched_recllm_records):
          Table.execute_functions(records)
          Table.update_stales(records, recllm_records)
      session.commit()

  @staticmethod
  def group_records(records):
    grouped_records = {}
    for record in records:
      if record.Table not in grouped_records:
        grouped_records[record.Table] = []
      grouped_records[record.Table].append(record)
    return grouped_records