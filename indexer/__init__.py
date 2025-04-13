from .db import Database
from .embed import Embedder



class Indexer:
  def __init__(self, config):
    self.config = config
    self.db = Database(config)
    self.embedder = Embedder()
  
  def index(self, rows, context_constructor, content_constructor):
    with self.db.Session() as session:
      session.add_all(rows)
      session.flush()

      contexts = []
      contents = []
      for row in rows:
        table = row.__class__.__table__
        context = context_constructor(row)
        content = content_constructor(row)
        contexts.append(context)
        contents.append(content)
      embeddings = self.embedder.embed(contents)

      for row, embedding, context in zip(rows, embeddings, contexts):
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
        # recllm object
        if recllm_type=='user':
          recllm_obj = self.db.RecLLMUsers(tablename=tablename, user_id=row.id, embedding=embedding, context=context)
        elif recllm_type=='item':
          recllm_obj = self.db.RecLLMItems(tablename=tablename, item_id=row.id, embedding=embedding, context=context)
        session.add(recllm_obj)
      session.commit()
