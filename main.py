from config.config import get_logger
from db.conn import get_engine

from pipeline.extract import Extract
from pipeline.transform import Transform

log = get_logger(__name__)

def main():
    
    print('=' * 60)
    log.info('iniciando Processo')
    print('=' * 60)
    
    engine = get_engine()
    if engine is None:
        raise RuntimeError('Não foi possivel conectar ao banco.')
    
    extract = Extract(engine)
    transform = Transform(engine)
    
    df = extract.process()
    if df is None:
        log.warning('Nenhum dado para processar. Encerrando...')
        exit(0)
    
    df_companies, df_snapshot = transform.process(df)


if __name__ == "__main__":
    main()
