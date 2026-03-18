import pandas as pd
import glob
import os

from config.config import DATA_RAW_PATH, OUTPUT_PATH, get_logger

log = get_logger(__name__)

class Extract:
    
    def __init__(self, engine):
        self.engine = engine
        
    
    def find_data(self) -> str | None:
        
        file = glob.glob(os.path.join(DATA_RAW_PATH, '*.xls*'))
        if len(file) == 0:
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
        
        log.info('Dados salvos')
        

    def cleanup(self):
        
        for file in glob.glob(os.path.join(DATA_RAW_PATH, '*.xls*')):
            os.remove(file)

        for file in glob.glob(os.path.join(OUTPUT_PATH, '*.parquet')):
            os.remove(file)
            
            
    def process(self) -> pd.DataFrame | None:
        
        filepath = self.find_data()
        
        if filepath is None:
            return None
        
        df = self.read_data(filepath)
        
        log.info('Salvando dados...')
        self.save_parquet(df)
        self.save_data(df)
        self.cleanup()

        log.info('Extração Completa!')
        return df