class Config:
  def __init__(self, user_tables, item_tables, embedding_dim):
    self.user_tables = user_tables # {'table_class': 'table_name'}
    self.item_tables = item_tables # {'table_class': 'table_name'}
    self.embedding_dim = embedding_dim