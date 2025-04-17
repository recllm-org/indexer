from .client import Client
from .utils import EnvVars



class Embedder:
  def embed(self, contents):
    raise NotImplementedError('embed must be implemented!')


class GeminiEmbedder(Embedder):
  def __init__(
    self, 
    model='gemini-embedding-exp-03-07', 
    task='SEMANTIC_SIMILARITY'
  ):
    self.model = model
    self.task = task
    self.embedding_dim = int(EnvVars.get('EMBEDDING_DIM'))
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


class CohereEmbedder(Embedder):
  def __init__(
    self,
    model='embed-v4.0',
    input_type='search_document', 
    embedding_types=['float']
  ):
    self.model = model
    self.input_type = input_type
    self.embedding_types = embedding_types
    self.client = Client.cohere()
  
  def embed(self, contents):
    result = self.client.embed(
      texts=contents,
      model=self.model,
      input_type=self.input_type,
      embedding_types=self.embedding_types
    )
    embeddings = [embedding for embedding in result.embeddings.float]
    return embeddings