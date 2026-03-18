import os 
import dotenv
import logging

dotenv.load_dotenv()

DATA_RAW_PATH = 'data/raw/'
OUTPUT_PATH = 'data/'
 
DB = os.getenv('DB')  
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')  
DB_HOST = os.getenv('DB_HOST') 
DB_PORT = os.getenv('DB_PORT')

COMPANIES_COLUMNS = [
    'Handle PubliBackup',
    'Nome Fantasia',
    'Razão Social',
    'CNPJ',
    'Telefone',
    'Telefone2',
    'Email',
    'Status Empresa',
    'Licença Ativa',
]

SNAPSHOT_COLUMNS = [
    'Handle PubliBackup',
    'Possui Backup',
    'Tempo Sem Backup',
    'Qtd. Total Arquivos',
    'Último Backup',
    'Data/Hora',
    'data_ingestao',
]

ANALYTICAL_COLUMNS = [
    'Handle PubliBackup',
    'Nome Fantasia',
    'Telefone',
    'Possui Backup',
    'Licença Ativa',
    'Último Backup',
    'Tempo Sem Backup',
    'Qtd. Total Arquivos',
    'Status Empresa',
    'data_ingestao',
]

def get_logger(name: str):

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    return logging.getLogger(name)