from .client import Client



class Database:
  def __init__(self, table):
    self.client = Client.supabase()
    self.table = table
  
  def push(self, rows):
    self.client.table(self.table).upsert(rows).execute()