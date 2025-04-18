from .embed import CohereEmbedder



class Function:
  def __init__(self, row_wise=True):
    self.row_wise = row_wise
  
  def fn(self, arg): # pass all kwargs required directly while implementing
    raise NotImplementedError('fn must be implemented!')
  
  def execute(self, rows):
    if self.row_wise:
      for row in rows:
        self.fn(row) # other kwargs will be passed directly
    else:
      self.fn(rows) # other kwargs will be passed directly


class ContentEmbedder(Function):
  def __init__(self, embedder):
    super().__init__(row_wise=False)
    if isinstance(embedder, CohereEmbedder) and embedder.multimodal:
      raise ValueError('Cohere multimodal embedder not supported in the default content embedder! Please define a custom embedder function!')
    self.embedder = embedder
  
  def fn(self, rows):
    all_contents = [row.cache.content for row in rows]
    embeddings = self.embedder.embed(all_contents)
    for row, embedding in zip(rows, embeddings):
      row.cache.embedding = embedding