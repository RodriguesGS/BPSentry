import sqlalchemy

from urllib.parse import quote_plus
from config.config import DB_HOST, DB_PORT, DB, DB_USER, DB_PASSWORD, get_logger

log = get_logger(__name__)

def get_engine():
    
    try:
        engine = sqlalchemy.create_engine(
            f'postgresql+psycopg2://{quote_plus(str(DB_USER))}:{quote_plus(str(DB_PASSWORD))}@{DB_HOST}:{DB_PORT}/{DB}'
        )
        
        return engine
    
    except Exception as err:
        log.error(f'Erro na conexão do banco: {err}')
        return None
    