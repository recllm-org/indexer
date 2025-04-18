from .client import Client
from .utils import EnvVars



class Embedder:
  def embed(self, contents):
    raise NotImplementedError('embed must be implemented!')


class GeminiEmbedder(Embedder):
  SUPPORTED_TASKS = [
    'SEMANTIC_SIMILARITY',
    'CLASSIFICATION',
    'CLUSTERING',
    'RETRIEVAL_DOCUMENT',
    'RETRIEVAL_QUERY',
    'QUESTION_ANSWERING',
    'FACT_VERIFICATION',
    'CODE_RETRIEVAL_QUERY'
  ]

  def __init__(
    self, 
    task,
    model='gemini-embedding-exp-03-07'
  ):
    self.model = model
    if task not in GeminiEmbedder.SUPPORTED_TASKS:
      raise ValueError(f'Task {task} is not supported! Choose from {GeminiEmbedder.SUPPORTED_TASKS}.')
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
  SUPPORTED_TASKS = [
    'search_document',
    'search_query',
    'classification',
    'clustering',
    'image'
  ]

  def __init__(
    self,
    task,
    model='embed-v4.0',
    embedding_types=['float']
  ):
    self.model = model
    if task not in CohereEmbedder.SUPPORTED_TASKS:
      raise ValueError(f'Task {task} is not supported! Choose from {CohereEmbedder.SUPPORTED_TASKS}.')
    self.input_type = task
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