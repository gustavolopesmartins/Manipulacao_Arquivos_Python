import os
from joblib import Parallel, delayed
from src import Extracao_CNAE
from backup_limpeza import backup_limpeza_simples
from descompactador import descompactador
from convertearquivo import convertearquivo
from datetime import datetime

# definindo os diretórios de quebradinha.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ESTABELECIMENTOSCSV = CURRENT_DIR.replace(r'GitHub\ETL_CNPJ\utilitarios',r'CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV')
BASES_CNAES = CURRENT_DIR.replace(r'utilitarios', r'Bases')

# difinindo nome para arquivo zipado
datazip = f'{datetime.now().day}-{datetime.now().month}-{datetime.now().year}'

# Filtra todos os arquivos csv da pasta
all_files_BASES_CNAES = list(filter(lambda x: '.csv' in x, os.listdir(BASES_CNAES)))

# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(all_files_BASES_CNAES) >= 1:
        backup_limpeza_simples(pasta=BASES_CNAES, nome_zipado=f'{BASES_CNAES}/Bases{datazip}.zip')
        
# Filtra todos os arquivos csv da pasta após o processo de descompactação e conversão aplicados
all_files_ESTABELECIMENTOSCSV = list(filter(lambda x: '.csv' in x, os.listdir(ESTABELECIMENTOSCSV)))

# Aplica a função Extração de empresas no palalelismo
Parallel(n_jobs=3,
	backend='threading', # multiprocessing
	verbose=10,
	batch_size=1024,
	prefer='threads',
	pre_dispatch='1 * n_jobs')(delayed(Extracao_CNAE)(file,ESTABELECIMENTOSCSV) for file in all_files_ESTABELECIMENTOSCSV)


"""
def processo(arquivo):
    Extracao_CNAE(file=arquivo, diretorio=ESTABELECIMENTOSCSV)
 
#if __name__ == '__main__':
with multiprocessing.Pool(processes=5) as pool:
	pool.map(processo, all_files_ESTABELECIMENTOSCSV)
"""