import pandas as pd
import glob
import sqlalchemy
import os

from config.config import DATA_RAW_PATH, OUTPUT_PATH, get_logger
from db.conn import get_engine

log = get_logger(__name__)

class Extract:
    
    def __init__(self):
        engine = get_engine()
        
        if engine is None:
            raise RuntimeError('Não foi possivel conectar ao banco.')
        
        self.engine: sqlalchemy.Engine = engine
    
    def find_data(self):
        
        file = glob.glob(os.path.join(DATA_RAW_PATH, '*.xls*'))

        try:
            if len(file) == 0:
                raise FileNotFoundError("Nenhum arquivo encontrado")
            
        except FileNotFoundError as err:
            log.error(f'Erro: {err}')
            return None
        
        log.info(f'Arquivo encontrado: {file[0]}')
        return file[0]
    
    def read_data(self, filepath) -> pd.DataFrame:

        df = pd.read_excel(filepath, engine='xlrd')
        
        df['data_ingestao'] = pd.Timestamp.today().normalize()
        df['CNPJ'] = df['CNPJ'].astype(str).str.zfill(14)
        
        log.info(f'Arquivo lido com sucesso: {df.shape[0]} linhas | {df.shape[1]} colunas')
        
        return df
    
    def save_parquet(self, df: pd.DataFrame):
           
        os.makedirs(OUTPUT_PATH, exist_ok=True)

        d = pd.Timestamp.today().strftime('%Y-%m-%d')
        path = os.path.join(OUTPUT_PATH, f'bpData_{d}.parquet')

        df.to_parquet(path, index=False)
        log.info(f'Parquet salvo em: {path}')
        
    
    def save_data(self, df: pd.DataFrame):

        df.to_sql(
            name='tb_raw',
            con=self.engine,
            schema='bronze',
            if_exists='append',
            index=False,
            chunksize=500,
        )
        
        log.info('Dados salvos em bronze.tb_raw')
        

    def process(self) -> pd.DataFrame:
        
        filepath = self.find_data()
        df       = self.read_data(filepath)
        
        log.info('Salvando dados...')
        self.save_parquet(df)
        self.save_data(df)

        log.info("Extração Completa!\n")
        return df