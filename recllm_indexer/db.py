from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from .utils import EnvVars
from .table import RecLLMBase



def get_connection_string():
  DB_USERNAME = EnvVars.get('DB_USERNAME')
  DB_PASSWORD = EnvVars.get('DB_PASSWORD')
  DB_HOST = EnvVars.get('DB_HOST')
  DB_PORT = EnvVars.get('DB_PORT')
  DB_NAME = EnvVars.get('DB_NAME')
  return f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    

class BasicDatabase:
  def __init__(self):
    self.engine = create_engine(get_connection_string())
    self.Session = sessionmaker(bind=self.engine)
  
  def pull_existing_tables(self, reqd_tables):
    metadata = MetaData()
    metadata.reflect(bind=self.engine)
    AutomapBase = automap_base(metadata=metadata)
    AutomapBase.prepare()
    existing_tables = {}
    for tablename in reqd_tables:
      existing_tables[tablename] = AutomapBase.classes[tablename]
    return existing_tables
    

class Database:
  def __init__(self, Tables):
    self.Tables = Tables
    # sqlalchemy
    self.engine = create_engine(get_connection_string())
    self.Session = sessionmaker(bind=self.engine)
    self.enable_vector_extension()
    # tables
    self.create_tables(RecLLMBase)
    # triggers
    self.create_triggers()
  
  def create_triggers(self):
    commands = []
    for Table in self.Tables:
      commands.append(Database.get_trigger_command(Table))
    unified_command = '\n'.join(commands)
    with self.Session() as session:
      session.execute(text(unified_command))
      session.commit()
  
  @staticmethod
  def get_trigger_command(Table):
    tablename = Table.SATable.__table__.name
    recllm_tablename = Table.RecLLMSATable.__table__.name
    tracked_columns = Table.tracked_columns
    
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
    DROP TRIGGER IF EXISTS {trigger_name} ON "{tablename}";
    -- Create the trigger
    CREATE TRIGGER {trigger_name}
    AFTER INSERT OR UPDATE OF {', '.join(tracked_columns)} OR DELETE ON "{tablename}"
    FOR EACH ROW
    EXECUTE FUNCTION {function_name}();
    """
    return command
  
  def enable_vector_extension(self):
    with self.Session() as session:
      session.execute(text('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA extensions;'))
      session.commit()
  
  def create_tables(self, Base):
    Base.metadata.create_all(self.engine)