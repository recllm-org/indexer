from .db import Database
from .table import UserTable, ItemTable
from .embed import Embedder
from .utils import SanitizedRow
from types import SimpleNamespace



class Indexer:
  def __init__(self, tables):
    Indexer.validate_tables(tables)
    self.db = Database(tables)
  
  def index(self, rows):
    mapping = Indexer.create_mapping(self.tables)
    grouped_rows = Indexer.group_rows(rows, mapping)
    with self.db.Session() as session:
      for tablename, rows in grouped_rows.items():
        table = mapping[tablename]
        table.execute_functions(rows)
        table.push(rows, session)
      session.commit()

  @staticmethod
  def group_rows(rows, mapping):
    grouped_rows = {}
    for row in rows:
      tablename = row.__class__.__table__.name
      # row modifications
      row.cache = SimpleNamespace()
      row = SanitizedRow(row, mapping[tablename].tracked_columns)
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
  
  @staticmethod
  def validate_tables(tables):
    for table in tables:
      if not isinstance(table, (UserTable, ItemTable)):
        raise ValueError(f'Table {table.tablename} should either be a UserTable or ItemTable!')