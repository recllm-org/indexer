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
    rows2embed = []
    rows2notembed = []
    for row in rows:
      if row.cache.content is not None:
        rows2embed.append(row)
      else:
        rows2notembed.append(row)

    if len(rows2embed)!=0:
      all_contents = [row.cache.content for row in rows2embed]
      embeddings = self.embedder.embed(all_contents)
      for row, embedding in zip(rows2embed, embeddings):
        row.cache.embedding = embedding
    for row in rows2notembed:
      row.cache.embedding = None