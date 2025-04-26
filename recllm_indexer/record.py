from types import SimpleNamespace



class Record:
  def __init__(self, row, table=None):
    self.__row = row
    self.table = table
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
    if (self.__is_locked) and (self.table is not None) and (attr not in self.table.tracked_columns):
      raise AttributeError(f'Column {attr} not found in tracked columns of table {self.table.SATable.__tablename__}!')
    return getattr(self.__row, attr)