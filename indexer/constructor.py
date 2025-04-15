class SanitizedRow:
  def __init__(self, row, config):
    self.config = config
    self.tablename = row.__class__.__table__.name
    self.tracked_columns = self.get_tracked_columns()
    SanitizedRow.set_required_attributes(self, row, self.tracked_columns)
  
  def get_tracked_columns(self):
    if self.tablename in self.config.user_tables:
      return self.config.user_tables[self.tablename].get('tracked_columns')
    elif self.tablename in self.config.item_tables:
      return self.config.item_tables[self.tablename].get('tracked_columns')
    else:
      raise ValueError(f'Table {self.tablename} not found in config!')
  
  @staticmethod
  def set_required_attributes(self, row, tracked_columns):
    for column in tracked_columns:
      setattr(self, column, getattr(row, column))
  
  def __getattr__(self, attr):
    ALLOWED_ATTRS = ['config', 'tablename', 'tracked_columns']
    if attr not in self.tracked_columns and attr not in ALLOWED_ATTRS:
      raise AttributeError(f'Column {attr} not found in tracked columns of table {self.tablename}!')
    return getattr(self, attr)


class Constructor:
  def __init__(self, constructor_fn_kwargs=None):
    self.constructor_fn_kwargs = constructor_fn_kwargs or {}
  
  def constructor_fn(self, sanitized_row, **kwargs):
    raise NotImplementedError('constructor_fn must be implemented!')
  
  def execute(self, row, config):
    sanitized_row = SanitizedRow(row, config)
    return self.constructor_fn(sanitized_row, **self.constructor_fn_kwargs)