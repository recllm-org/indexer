from .db import Database
from .embed import Embedder



class Indexer:
  def __init__(self, config):
    self.config = config
    self.db = Database(config)
    self.embedder = Embedder()
  
  def index(self, rows, constructor):
    for row in rows:
      contents = constructor(row, self.db.table.__table__.columns)
      embedding = self.embedder.embed(contents)
      row.embedding = embedding
    self.db.push(rows)