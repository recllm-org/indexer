"""
Wrapper around a SQLAlchemy table row
"""



from types import SimpleNamespace



class Record:
  """
  Wrapper around a SQLAlchemy table row
    - `row` is a SQLAlchemy table row
    - `cache` is a namespace for storing cached values
    - `lock` and `unlock` are used to lock and unlock the record
  
  Attribute access
    - getter to access row attributes as record attributes
    - returns the value if (it is not locked) or (if the `Table` is not set) or (if the attribute is part of the tracked columns of the table)
    - raises an error otherwise

  Locking
    - Used to restrict access to columns that are not part of the tracked columns of the table
    - To retrieve the row, the record must be unlocked
  """
  
  def __init__(self, row, Table=None):
    self.__row = row
    self.Table = Table
    self.cache = SimpleNamespace()
    self.__is_locked = False
  
  def lock(self):
    self.__is_locked = True
  
  def unlock(self):
    self.__is_locked = False
  
  def get_row(self):
    if self.__is_locked:
      raise AttributeError(f'Record {self} is locked!')
    return self.__row
  
  def __getattr__(self, attr):
    if (self.__is_locked) and (self.Table is not None) and (attr not in self.Table.tracked_columns):
      raise AttributeError(f'Column {attr} not found in tracked columns of table {self.Table.SATable.__tablename__}!')
    return getattr(self.__row, attr)