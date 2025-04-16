from .client import Client
from .utils import get_envars



envars = get_envars()
embedding_dim = int(envars.get('EMBEDDING_DIM'))


class GeminiEmbedder:
  def __init__(self, model='gemini-embedding-exp-03-07', task='SEMANTIC_SIMILARITY'):
    self.model = model
    self.task = task
    self.embedding_dim = embedding_dim
    self.client = Client.gemini()
  
  def embed(self, contents):
    result = self.client.models.embed_content(
      model=self.model, 
      contents=contents,
      config={
        'task_type': self.task, 
        'output_dimensionality': self.embedding_dim
      }
    )
    embeddings = [embedding.values for embedding in result.embeddings]
    return embeddings