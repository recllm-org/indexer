from .table import UserTable, ItemTable
from dotenv import dotenv_values



class Config:
  def __init__(self, tables, model='gemini-embedding-exp-03-07', task='SEMANTIC_SIMILARITY', embedding_dim=1536):
    self.tables = tables
    self.model = model
    self.task = task
    self.embedding_dim = embedding_dim
    self.validate_tables(tables)
    self.mapping = self.create_mapping(tables)
    self.envars = dotenv_values('.env')
  
  def create_mapping(self, tables):
    mapping = {}
    for table in tables:
      mapping[table.tablename] = table
    return mapping

  def validate_tables(self, tables):
    for table in tables:
      if not isinstance(table, (UserTable, ItemTable)):
        raise ValueError(f'Table {table.tablename} should either be a UserTable or ItemTable!')