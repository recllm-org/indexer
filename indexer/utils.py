from dotenv import dotenv_values



class SanitizedRow:
  def __init__(self, row, tracked_columns):
    self.__row = row
    self.__tracked_columns = tracked_columns
    self.__is_locked = True
  
  def unlock(self):
    self.__is_locked = False
  
  def __getattr__(self, attr):
    if self.__is_locked and attr not in self.__tracked_columns:
      raise AttributeError(f'Column {attr} not found in tracked columns of table {self.__tablename}!')
    return getattr(self.__row, attr)


def get_envars():
  envars = dotenv_values('.env')
  return envars