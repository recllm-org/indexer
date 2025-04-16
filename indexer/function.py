class SanitizedRow:
  def __init__(self, row, config):
    self.config = config
    self.tablename = row.__class__.__table__.name
    self.tracked_columns = config.mapping[self.tablename].tracked_columns
    SanitizedRow.set_required_attributes(self, row, self.tracked_columns)
  
  @staticmethod
  def set_required_attributes(self, row, tracked_columns):
    for column in tracked_columns:
      setattr(self, column, getattr(row, column))
  
  def __getattr__(self, attr):
    ALLOWED_ATTRS = ['config', 'tablename', 'tracked_columns']
    if attr not in self.tracked_columns and attr not in ALLOWED_ATTRS:
      raise AttributeError(f'Column {attr} not found in tracked columns of table {self.tablename}!')
    return getattr(self, attr)


class Function:
  def __init__(self, kwargs=None):
    self.kwargs = kwargs or {}
  
  def fn(self, row, **kwargs):
    raise NotImplementedError('fn must be implemented!')
  
  def execute(self, rows, config):
    for row in rows:
      sanitized_row = SanitizedRow(row, config)
      return self.fn(sanitized_row, **self.kwargs)