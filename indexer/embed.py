from .client import Client



class Embedder:
  def __init__(self, config):
    self.config = config
    self.client = Client.gemini()
    self.model = config.model
    self.task = config.task
  
  def embed(self, contents):
    result = self.client.models.embed_content(model=self.model, contents=contents, config={'task_type': self.task, 'output_dimensionality': self.config.embedding_dim})
    embeddings = [embedding.values for embedding in result.embeddings]
    return embeddings