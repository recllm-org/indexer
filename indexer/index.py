from .db import Database
from .utils import SanitizedRow
from types import SimpleNamespace



class Indexer:
  def __init__(self, tables):
    self.mapping = Indexer.create_mapping(tables)
    self.db = Database(tables)
  
  def index(self, rows):
    grouped_rows = self.group_rows(rows)
    with self.db.Session() as session:
      for tablename, rows in grouped_rows.items():
        table = self.mapping[tablename]
        table.execute_functions(rows)
        table.push(rows, session)
      session.commit()

  def group_rows(self, rows):
    grouped_rows = {}
    for row in rows:
      tablename = row.__class__.__table__.name
      # row modifications
      row.cache = SimpleNamespace()
      row = SanitizedRow(row, tablename, self.mapping[tablename].tracked_columns)
      # grouping
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