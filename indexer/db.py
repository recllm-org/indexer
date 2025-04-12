from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker, mapped_column, DeclarativeBase
from sqlalchemy.ext.automap import automap_base
from pgvector.sqlalchemy import Vector
from .client import Client
from dotenv import load_dotenv



class Database:
  def __init__(self, config):
    self.config = config
    # supabase
    self.client = Client.supabase()
    # sqlalchemy
    self.engine = create_engine(Database.get_connection_string())
    self.Session = sessionmaker(bind=self.engine)
    self.metadata = MetaData(self.engine)
    self.metadata.reflect()
    # tables
    self.get_existing_tables(self.config.user_tables)
    self.get_existing_tables(self.config.item_tables)
    self.RecLLMUser, self.RecLLMItem = self.get_or_create_recllm_tables()
  
  def push(self, rows):
    self.client.table(self.config.table_name).upsert(rows).execute()
  
  def get_or_create_recllm_tables(self):
    class Base(DeclarativeBase): metadata = self.metadata
    # users
    class RecLLMUser(Base):
      __tablename__ = 'recllm_users'
      id = mapped_column(Integer, primary_key=True)
      table = mapped_column(String)
      user_id = mapped_column(Integer)
      embedding = mapped_column(Vector(dimensions=self.config.embedding_dim))
    # items
    class RecLLMItem(Base):
      __tablename__ = 'recllm_items'
      id = mapped_column(Integer, primary_key=True)
      table = mapped_column(String)
      item_id = mapped_column(Integer)
      embedding = mapped_column(Vector(dimensions=self.config.embedding_dim))
      summary = mapped_column(String)
    self.metadata.create_all(self.engine)
    return RecLLMUser, RecLLMItem
  
  def get_existing_tables(self, tables):
    Base = automap_base(self.metadata)
    Base.prepare()
    for table_class, table_name in tables.items():
      self.__setattr__(table_class, Base.classes[table_name])
  
  @staticmethod
  def get_connection_string():
    envars = load_dotenv('.env')
    DB_USERNAME = envars.get('DB_USERNAME')
    DB_PASSWORD = envars.get('DB_PASSWORD')
    DB_HOST = envars.get('DB_HOST')
    DB_PORT = envars.get('DB_PORT')
    DB_NAME = envars.get('DB_NAME')
    return f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'