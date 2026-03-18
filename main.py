from config.config import get_logger
from db.conn import get_engine

from pipeline.extract import Extract
from pipeline.transform import Transform
from pipeline.load import Load

log = get_logger(__name__)

def main():
    
    print('=' * 70)
    log.info('iniciando Processo')
    print('=' * 70)
    
    engine = get_engine()
    if engine is None:
        raise RuntimeError('Não foi possivel conectar ao banco.')
    
    extract   = Extract(engine=engine)
    transform = Transform(engine=engine)
    load      = Load(engine=engine)
    
    df = extract.process()
    if df is None:
        log.warning('Nenhum dado para processar. Encerrando...')
        exit(0)
    
    df_companies, df_snapshot = transform.process(df=df)
    if df_snapshot is None:
        log.info('Nenhum dado novo para processar. Encerrando...')
        exit(0)
        
    load.process(df_companies=df_companies, df_snapshot=df_snapshot)
    
    print('=' * 70)
    log.info('Processo finalizado com sucesso!!')
    print('=' * 70)


if __name__ == "__main__":
    main()
