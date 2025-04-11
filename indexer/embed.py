from .client import Client



class Embedder:
  def __init__(self, model='gemini-embedding-exp-03-07', task='SEMANTIC_SIMILARITY'):
    self.client = Client.gemini()
    self.model = model
    self.task = task
  
  def embed(self, contents):
    result = self.client.models.embed_content(model=self.model, contents=contents, task_type=self.task)
    return result.embeddings