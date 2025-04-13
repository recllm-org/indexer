class Config:
  def __init__(self, user_tables, item_tables, model='gemini-embedding-exp-03-07', task='SEMANTIC_SIMILARITY', embedding_dim=1536):
    self.user_tables = user_tables # {'table_name': 'table_class'}
    self.item_tables = item_tables # {'table_name': 'table_class'}
    self.model = model
    self.task = task
    self.embedding_dim = embedding_dim