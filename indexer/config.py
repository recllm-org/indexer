class Config:
  def __init__(self, table_name, embedding_dim, use_existing_table):
    self.table_name = table_name
    self.embedding_dim = embedding_dim
    self.use_existing_table = use_existing_table
    self.columns = []
  
  def add_columns(self, columns):
    if not self.use_existing_table:
      self.columns.extend(columns)
    else:
      raise ValueError('use_existing_table is True, so columns cannot be added!')