"""
Embeds contents into vector embeddings
Choose from the most suitable supported task for your use case

Gemini
  - Text

Cohere
  - Text
  - Images
"""



from .client import Client



class Embedder:
  def __init__(self, task, embedding_dim, model):
    cls = type(self)
    if task not in cls.SUPPORTED_TASKS:
      raise ValueError(f'Task {task} is not supported! Choose from {cls.SUPPORTED_TASKS}.')
    self.task = task
    self.embedding_dim = embedding_dim
    self.model = model
  
  def embed(self, contents):
    raise NotImplementedError('embed must be implemented!')


class GeminiEmbedder(Embedder):
  """
  Supported tasks:
    SEMANTIC_SIMILARITY
    CLASSIFICATION
    CLUSTERING
    RETRIEVAL_DOCUMENT
    RETRIEVAL_QUERY
    QUESTION_ANSWERING
    FACT_VERIFICATION
    CODE_RETRIEVAL_QUERY
  """

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
    embedding_dim,
    model='gemini-embedding-exp-03-07',
  ):
    super().__init__(task, embedding_dim, model)
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
  """
  Supported tasks:
    search_document
    search_query
    classification
    clustering
    image
    
  Enable multimodal embeddings by setting `multimodal=True`
  """

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
    embedding_dim,
    model='embed-v4.0',
    multimodal=False,
    embedding_types=['float']
  ):
    super().__init__(task, embedding_dim, model)
    self.multimodal = multimodal
    self.embedding_types = embedding_types
    self.client = Client.cohere()
  
  def embed(self, contents):
    kwargs = {'inputs': contents} if self.multimodal else {'texts': contents}
    result = self.client.embed(
      **kwargs,
      model=self.model,
      input_type=self.task,
      embedding_types=self.embedding_types
    )
    embeddings = [embedding for embedding in result.embeddings.float]
    return embeddings