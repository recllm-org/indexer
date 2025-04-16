from .client import Client
from .utils import get_envars



class Embedder:
  def __init__(self):
    envars = get_envars()
    self.model = envars.get('GEMINI_MODEL')
    self.task = envars.get('GEMINI_TASK')
    self.embedding_dim = int(envars.get('GEMINI_EMBEDDING_DIM'))
    self.client = Client.gemini()
  
  def embed(self, contents):
    result = self.client.models.embed_content(
      model=self.model, contents=contents,
      config={
        'task_type': self.task, 
        'output_dimensionality': self.embedding_dim
      }
    )
    embeddings = [embedding.values for embedding in result.embeddings]
    return embeddings