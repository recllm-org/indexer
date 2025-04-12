from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from pgvector.sqlalchemy import Vector
from .client import Client
from dotenv import load_dotenv



class Database:
  def __init__(self, config):
    envars = load_dotenv('.env')
    self.config = config
    # supabase
    DB_USERNAME = envars.get('DB_USERNAME')
    DB_PASSWORD = envars.get('DB_PASSWORD')
    DB_HOST = envars.get('DB_HOST')
    DB_PORT = envars.get('DB_PORT')
    DB_NAME = envars.get('DB_NAME')
    self.client = Client.supabase()
    # sqlalchemy
    self.engine = create_engine(f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    self.Session = sessionmaker(bind=self.engine)
    self.metadata = MetaData()
    self.metadata.reflect(bind=self.engine)
    self.Base = declarative_base()
    self.table = self.get_or_create_table()
  
  def get_or_create_table(self):
    embedding_column = Column('embedding', Vector(dimensions=self.config.embedding_dim))
    extended_columns = self.config.columns + [embedding_column]
    table = Table(self.config.table_name, self.metadata, *extended_columns, extend_existing=True, autoload_with=self.engine)
    self.metadata.create_all(self.engine)
    return table
  
  def push(self, rows):
    self.client.table(self.config.table_name).upsert(rows).execute()