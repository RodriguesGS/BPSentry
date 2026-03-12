from config.config import get_logger
from pipeline.extract import Extract

log = get_logger(__name__)

def main():
    
    print('=' * 60)
    log.info('iniciando Processo')
    print('=' * 60 + '\n')
    
    log.info('Iniciando Extração...')
    extract = Extract()
    extract.process()


if __name__ == "__main__":
    main()
