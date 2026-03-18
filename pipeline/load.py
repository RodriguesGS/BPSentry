import pandas as pd

from config.config import ANALYTICAL_COLUMNS, get_logger

log = get_logger(__name__)

class Load:

    def __init__(self, engine):
        self.engine = engine

    def build_analytical(self, df_companies: pd.DataFrame, df_snapshot: pd.DataFrame) -> pd.DataFrame:

        df = df_companies.merge(
            df_snapshot,
            on='Handle PubliBackup',
            how='inner'
        )

        log.info(f'{len(df)} registros completos')
        return df[ANALYTICAL_COLUMNS]

    def save_analytical(self, df: pd.DataFrame):

        existing = pd.read_sql(
            'SELECT "Handle PubliBackup", data_ingestao FROM gold.tb_analytical',
            con=self.engine
        )

        df_new = df[~df['Handle PubliBackup'].isin(existing['Handle PubliBackup']) |
                    ~df['data_ingestao'].isin(existing['data_ingestao'])]

        if df_new.empty:
            log.info('Base analítica do dia já existe')
            return

        df_new.to_sql(
            name='tb_analytical',
            con=self.engine,
            schema='gold',
            if_exists='append',
            index=False,
            chunksize=500,
        )
        log.info('Dados salvos')

    def process(self, df_companies: pd.DataFrame, df_snapshot: pd.DataFrame):

        log.info('Iniciando Load...')

        df_analytical = self.build_analytical(df_companies, df_snapshot)
        self.save_analytical(df_analytical)

        log.info('Load Completo!')
        