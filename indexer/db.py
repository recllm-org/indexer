from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, text
from sqlalchemy.orm import sessionmaker, mapped_column, DeclarativeBase
from sqlalchemy.ext.automap import automap_base
from pgvector.sqlalchemy import Vector
from .client import Client
from dotenv import dotenv_values



class Database:
  def __init__(self, config):
    self.config = config
    # supabase
    self.client = Client.supabase()
    # sqlalchemy
    self.engine = create_engine(Database.get_connection_string())
    self.Session = sessionmaker(bind=self.engine)
    self.metadata = MetaData()
    self.metadata.reflect(bind=self.engine)
    self.enable_vector_extension()
    # tables
    self.get_existing_tables(self.config.user_tables)
    self.get_existing_tables(self.config.item_tables)
    self.get_or_create_recllm_tables()
    # triggers
    self.create_triggers()
  
  def get_or_create_recllm_tables(self):
    class Base(DeclarativeBase): metadata = self.metadata
    # users
    class RecLLMUsers(Base):
      __tablename__ = 'recllm_users'
      __table_args__ = {'extend_existing': True}
      id = mapped_column(Integer, primary_key=True, autoincrement=True)
      tablename = mapped_column(String)
      row_id = mapped_column(Integer)
      embedding = mapped_column(Vector(self.config.embedding_dim))
      context = mapped_column(String)
      stale = mapped_column(Boolean, default=False)
    # items
    class RecLLMItems(Base):
      __tablename__ = 'recllm_items'
      __table_args__ = {'extend_existing': True}
      id = mapped_column(Integer, primary_key=True, autoincrement=True)
      tablename = mapped_column(String)
      row_id = mapped_column(Integer)
      embedding = mapped_column(Vector(self.config.embedding_dim))
      context = mapped_column(String)
      stale = mapped_column(Boolean, default=False)
    self.metadata.create_all(self.engine)
    self.RecLLMUsers = RecLLMUsers
    self.RecLLMItems = RecLLMItems
  
  def get_existing_tables(self, tables):
    Base = automap_base(metadata=self.metadata)
    Base.prepare()
    for tablename, table_details in tables.items():
      self.__setattr__(table_details['class'], Base.classes[tablename])
  
  def create_triggers(self):
    commands = []
    for tablename in self.config.user_tables:
      commands.append(self.get_trigger_command(tablename))
    for tablename in self.config.item_tables:
      commands.append(self.get_trigger_command(tablename))
    unified_command = '\n'.join(commands)
    with self.Session() as session:
      session.execute(text(unified_command))
      session.commit()
  
  def get_trigger_command(self, tablename):
    recllm_tablename = None
    tracked_columns = None
    if tablename in self.config.user_tables:
      recllm_tablename = self.RecLLMUsers.__tablename__
      tracked_columns = self.config.user_tables[tablename]['tracked_columns']
    elif tablename in self.config.item_tables:
      recllm_tablename = self.RecLLMItems.__tablename__
      tracked_columns = self.config.item_tables[tablename]['tracked_columns']
    else:
      raise ValueError(f'Table {tablename} not found in config!')
    
    trigger_name = f'recllm_trigger_{tablename}'
    function_name = f'recllm_fn_{tablename}'
    command = f"""
    -- Create or replace the trigger function
    CREATE OR REPLACE FUNCTION {function_name}()
    RETURNS TRIGGER AS $$
    BEGIN
        IF TG_OP = 'UPDATE' THEN
            -- Update the stale column in {recllm_tablename} table
            UPDATE {recllm_tablename}
            SET stale = TRUE
            WHERE tablename = '{tablename}' AND row_id = OLD.id;
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            -- Delete the corresponding row in {recllm_tablename} table
            DELETE FROM {recllm_tablename}
            WHERE tablename = '{tablename}' AND row_id = OLD.id;
            RETURN OLD;
        ELSIF TG_OP = 'INSERT' THEN
            -- Insert a new row into {recllm_tablename} table
            INSERT INTO {recllm_tablename} (tablename, row_id, stale)
            VALUES ('{tablename}', NEW.id, TRUE);
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    -- Drop the trigger if it already exists
    DROP TRIGGER IF EXISTS {trigger_name} ON {tablename};
    -- Create the trigger
    CREATE TRIGGER {trigger_name}
    AFTER INSERT OR UPDATE OF {', '.join(tracked_columns)} OR DELETE ON {tablename}
    FOR EACH ROW
    EXECUTE FUNCTION {function_name}();
    """
    return command
  
  def enable_vector_extension(self):
    with self.Session() as session:
      session.execute(text('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA extensions;'))
      session.commit()
  
  @staticmethod
  def get_connection_string():
    envars = dotenv_values('.env')
    DB_USERNAME = envars.get('DB_USERNAME')
    DB_PASSWORD = envars.get('DB_PASSWORD')
    DB_HOST = envars.get('DB_HOST')
    DB_PORT = envars.get('DB_PORT')
    DB_NAME = envars.get('DB_NAME')
    return f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'