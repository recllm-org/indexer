from dotenv import dotenv_values
import os



class SanitizedRow:
  def __init__(self, row, tablename, tracked_columns):
    self.__row = row
    self.__tablename = tablename
    self.__tracked_columns = tracked_columns
    self.__is_locked = True
  
  def unlock(self):
    self.__is_locked = False
  
  def __getattr__(self, attr):
    ALLOWED_ATTRIBUTES = ['cache']
    if ((self.__is_locked) and (attr not in [*self.__tracked_columns, *ALLOWED_ATTRIBUTES])):
      raise AttributeError(f'Column {attr} not found in tracked columns of table {self.__tablename}!')
    return getattr(self.__row, attr)


class EnvVars:
  envars = dotenv_values('.env')
  
  @staticmethod
  def get(key, include_os=True): # include_os is useful if there are namespace conflicts, ie same key in .env and os.environ
    if include_os:
      return EnvVars.envars.get(key) or os.environ.get(key)
    else:
      return EnvVars.envars.get(key)