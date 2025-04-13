from .db import Database
from .embed import Embedder



class Indexer:
  def __init__(self, config):
    self.config = config
    self.db = Database(config)
    self.embedder = Embedder()
  
  def index(self, rows, constructor):
    with self.db.Session() as session:
      for row in rows:
        session.add(row)
        session.flush()
        session.refresh(row)

        recllm_type = None
        tablename = row.__class__.__tablename__
        table = row.__class__.__table__
        if tablename in self.config.user_tables:
          recllm_type = 'user'
        elif tablename in self.config.item_tables:
          recllm_type = 'item'
        else:
          raise ValueError(f'Invalid table: {tablename}. Table should either be in user_tables or item_tables.')
        
        contents = constructor(row, table.columns.keys())
        embedding = self.embedder.embed(contents)
        if recllm_type=='user':
          recllm_obj = self.db.RecLLMUsers(tablename=tablename, user_id=row.id, embedding=embedding)
        elif recllm_type=='item':
          recllm_obj = self.db.RecLLMItems(tablename=tablename, item_id=row.id, embedding=embedding)
        else:
          raise ValueError(f'Invalid recllm_type: {recllm_type}. Expected user or item.')
        session.add(recllm_obj)
      session.commit()
