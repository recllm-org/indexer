from .db import Database
from .embed import Embedder
from sqlalchemy import text



class Indexer:
  def __init__(self, config):
    self.config = config
    self.db = Database(config)
    self.embedder = Embedder(config)
  
  def index(self, rows, context_constructor, content_constructor, context_constructor_kwargs=None, content_constructor_kwargs=None):
    context_constructor_kwargs = context_constructor_kwargs or {}
    content_constructor_kwargs = content_constructor_kwargs or {}
    with self.db.Session() as session:
      session.execute(text('SET session_replication_role = replica')) # prevent triggers from firing
      session.add_all(rows)
      session.flush()

      contexts = []
      contents = []
      for row in rows:
        context = context_constructor(row, **context_constructor_kwargs)
        content = content_constructor(row, **content_constructor_kwargs)
        contexts.append(context)
        contents.append(content)
      embeddings = self.embedder.embed(contents)

      recllm_objs = []
      for row, embedding, context in zip(rows, embeddings, contexts):
        session.refresh(row)
        recllm_type = None
        tablename = row.__class__.__table__.name
        if tablename in self.config.user_tables:
          recllm_type = 'user'
        elif tablename in self.config.item_tables:
          recllm_type = 'item'
        else:
          raise ValueError(f'Invalid table: {tablename}. Table should either be in user_tables or item_tables.')
        # recllm object
        if recllm_type=='user':
          recllm_obj = self.db.RecLLMUsers(tablename=tablename, row_id=row.id, embedding=embedding, context=context)
        elif recllm_type=='item':
          recllm_obj = self.db.RecLLMItems(tablename=tablename, row_id=row.id, embedding=embedding, context=context)
        recllm_objs.append(recllm_obj)
      session.add_all(recllm_objs)
      session.commit()