from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from .table import Base, RecLLMUsers, RecLLMItems
from .client import Client


class Database:
  def __init__(self, config):
    self.config = config
    # supabase
    self.client = Client.supabase()
    # sqlalchemy
    self.engine = create_engine(self.get_connection_string())
    self.Session = sessionmaker(bind=self.engine)
    self.enable_vector_extension()
    # tables
    self.get_existing_tables(self.config.tables)
    # recllm tables
    Base.metadata.create_all(self.engine)
    self.RecLLMUsers = RecLLMUsers
    self.RecLLMItems = RecLLMItems
    # triggers
    self.create_triggers()
  
  def get_existing_tables(self, tables):
    metadata = MetaData()
    metadata.reflect(bind=self.engine)
    AutomapBase = automap_base(metadata=metadata)
    AutomapBase.prepare()
    for table in tables:
      self.__setattr__(table.classname, AutomapBase.classes[table.tablename])
  
  def create_triggers(self):
    commands = []
    for table in self.config.tables:
      commands.append(self.get_trigger_command(table))
    unified_command = '\n'.join(commands)
    with self.Session() as session:
      session.execute(text(unified_command))
      session.commit()
  
  def get_trigger_command(self, table):
    tablename = table.tablename
    tracked_columns = table.tracked_columns
    recllm_tablename = table.recllm_tablename
    
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
  
  def get_connection_string(self):
    envars = self.config.envars
    DB_USERNAME = envars.get('DB_USERNAME')
    DB_PASSWORD = envars.get('DB_PASSWORD')
    DB_HOST = envars.get('DB_HOST')
    DB_PORT = envars.get('DB_PORT')
    DB_NAME = envars.get('DB_NAME')
    return f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'