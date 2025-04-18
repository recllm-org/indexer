from .db import Database
from .utils import SanitizedRow
from types import SimpleNamespace



class Indexer:
  def __init__(self, tables):
    self.mapping = Indexer.create_mapping(tables)
    self.db = Database(tables)
  
  def index(self, rows):
    rows = self.init_rows(rows)
    grouped_rows = self.group_rows(rows)
    with self.db.Session() as session:
      for tablename, rows in grouped_rows.items():
        table = self.mapping[tablename]
        table.execute_functions(rows)
        table.push(rows, session)
      session.commit()
  
  def update_stales(self, batch_size=25):
    with self.db.Session() as session:
      for table in self.mapping.values():
        table_class = self.db.__getattribute__(table.classname)
        batched_rows, batched_recllm_rows = table.retrieve_stales(table_class, session, batch_size)
        for rows, recllm_rows in zip(batched_rows, batched_recllm_rows):
          rows = self.init_rows(rows)
          table.execute_functions(rows)
          table.update_stales(rows, recllm_rows)
      session.commit()
  
  def init_rows(self, rows):
    sanitized_rows = []
    for row in rows:
      tablename = row.__class__.__table__.name
      row.cache = SimpleNamespace()
      row = SanitizedRow(row, tablename, self.mapping[tablename].tracked_columns)
      sanitized_rows.append(row)
    return sanitized_rows

  def group_rows(self, rows):
    grouped_rows = {}
    for row in rows:
      tablename = row._tablename
      if tablename not in grouped_rows:
        grouped_rows[tablename] = []
      grouped_rows[tablename].append(row)
    return grouped_rows
  
  @staticmethod
  def create_mapping(tables):
    mapping = {}
    for table in tables:
      mapping[table.tablename] = table
    return mapping