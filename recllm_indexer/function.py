"""
`Function` modifies records either for each record separately or for all records at once
  - Set `record_wise=True` to modify each record separately
  - Set `record_wise=False` to modify all records at once
  - `fn` must be implemented by the subclass

`ContentEmbedder` is the default function for embedding contents into vector embeddings
"""



from .embed import CohereEmbedder



class Function:
  def __init__(self, record_wise=True):
    self.record_wise = record_wise
  
  def fn(self, arg):
    raise NotImplementedError('fn must be implemented!')
  
  def execute(self, records):
    if self.record_wise:
      for record in records:
        self.fn(record)
    else:
      self.fn(records)


class ContentEmbedder(Function):
  """
  Embeds contents into vector embeddings
    - Default embedder for embedding text only
    - Only embeds `record.cache.content` of records only if they are not `None`
    - Embeddings for each record are stored in `record.cache.embedding`
  """

  def __init__(self, embedder):
    super().__init__(record_wise=False)
    if isinstance(embedder, CohereEmbedder) and embedder.multimodal:
      raise ValueError('Cohere multimodal embedder not supported in the default content embedder! Please define a custom embedder function!')
    self.embedder = embedder
  
  def fn(self, records):
    records2embed = []
    records2notembed = []
    for record in records:
      if record.cache.content is not None:
        records2embed.append(record)
      else:
        records2notembed.append(record)

    if len(records2embed)!=0:
      all_contents = [record.cache.content for record in records2embed]
      embeddings = self.embedder.embed(all_contents)
      for record, embedding in zip(records2embed, embeddings):
        record.cache.embedding = embedding
    for record in records2notembed:
      record.cache.embedding = None