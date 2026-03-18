import pandas as pd

from config.config import COMPANIES_COLUMNS, SNAPSHOT_COLUMNS, get_logger


log = get_logger(__name__)

class Transform:

    def __init__(self, engine):
        self.engine = engine
        

    def build_tb(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        
        df_companies = df[COMPANIES_COLUMNS].drop_duplicates(subset=['Handle PubliBackup'])
        df_snapshot = df[SNAPSHOT_COLUMNS]
                                                            
        return df_companies, df_snapshot
    
    
    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:

        df_filtered = df[
            (df['Licença Ativa'] == 1) &
            (df['Possui Backup'] == 1) &
            (df['Status Empresa'] == 'Ativo')
        ]

        log.info(f'{len(df_filtered)} registros após filtro de {len(df)} total')

        return df_filtered

    def save_companies(self, df: pd.DataFrame):

        existing = pd.read_sql(
            'SELECT "Handle PubliBackup" FROM silver.tb_companies',
            con=self.engine
        )

        df_new = df[~df['Handle PubliBackup'].isin(existing['Handle PubliBackup'])]

        if df_new.empty:
            log.info('Nenhuma empresa nova para inserir')
            return

        df_new.to_sql(
            name='tb_companies',
            con=self.engine,
            schema='silver',
            if_exists='append',
            index=False,
            chunksize=500,
        )
        log.info(f'{len(df_new)} empresas novas foram salvas')


    def save_snapshot(self, df: pd.DataFrame):

        existing = pd.read_sql(
            'SELECT "Handle PubliBackup", data_ingestao FROM silver.tb_snapshot',
            con=self.engine
        )

        df_new = df[~df['Handle PubliBackup'].isin(existing['Handle PubliBackup']) |
                    ~df['data_ingestao'].isin(existing['data_ingestao'])]

        if df_new.empty:
            log.info('Ingestão do dia já foi feita')
            return

        df_new.to_sql(
            name='tb_snapshot',
            con=self.engine,
            schema='silver',
            if_exists='append',
            index=False,
            chunksize=500,
        )
        log.info(f'{len(df_new)} registros foram salvos')

    def process(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
         
        log.info('Iniciando Transform...')
        df = self.filter_data(df)
        df_companies, df_snapshot = self.build_tb(df)

        self.save_companies(df_companies)
        self.save_snapshot(df_snapshot)

        log.info('Transform Completo!')

        return df_companies, df_snapshot
    