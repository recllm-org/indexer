class Config:
  def __init__(self, user_tables, item_tables, model='gemini-embedding-exp-03-07', task='SEMANTIC_SIMILARITY', embedding_dim=1536):
    self.user_tables = user_tables # {tablename: {'class': table_class, 'tracked_columns': [column_name]}}
    self.item_tables = item_tables # {tablename: {'class': table_class, 'tracked_columns': [column_name]}}
    self.model = model
    self.task = task
    self.embedding_dim = embedding_dim