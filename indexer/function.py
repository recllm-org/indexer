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