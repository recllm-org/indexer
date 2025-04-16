from .db import Database
from .embed import Embedder



class Indexer:
  def __init__(self, config):
    self.config = config
    self.db = Database(config)
    self.embedder = Embedder(config)
  
  def index(self, rows):
    pass