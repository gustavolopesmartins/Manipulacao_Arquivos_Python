import os
import csv
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from joblib import Parallel, delayed
from src import Extracao_CNAE
from backup_limpeza import backup_limpeza_simples
from descompactador import descompactador
from convertearquivo import convertearquivo
from datetime import datetime
# definindo os diretórios de quebradinha.
ESTABELECIMENTOSZIP = r"C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSZIP"
ESTABELECIMENTOSCSV =r"C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV"
Bases_CNAES = r'../Bases/'
# difinindo datazip
datazip = datazip = f'{datetime.now().month-1}-{datetime.now().year}'

# Filtra todos os arquivos csv da pasta
all_files_Bases_CNAES = list(filter(lambda x: '.csv' in x, os.listdir(Bases_CNAES)))
# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(all_files_Bases_CNAES) >= 1:
        backup_limpeza_simples(pasta=Bases_CNAES, nome_zipado=f'{Bases_CNAES}Bases_CNAES_{datazip}.zip')
# Filtra todos os arquivos csv da pasta
all_files_ESTABELECIMENTOSCSV = list(filter(lambda x: '.csv' in x, os.listdir(ESTABELECIMENTOSCSV)))
# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(all_files_ESTABELECIMENTOSCSV) >= 1:
    backup_limpeza_simples(pasta=ESTABELECIMENTOSCSV, nome_zipado=f'{ESTABELECIMENTOSCSV}/periodos anteriores/datazip_ESTABELECIMENTOSCSV.zip')

# descompacta os arquivos em zip antes de extrair as empresas
descompactador(diretorioatual=ESTABELECIMENTOSZIP, diretoriodestino=ESTABELECIMENTOSCSV)
convertearquivo(diretorio=ESTABELECIMENTOSCSV,tipo_atual='ESTABELE', tipo_destino='csv')

# Filtra todos os arquivos csv da pasta após o processo de descompactação e conversão aplicados
all_files_ESTABELECIMENTOSCSV = list(filter(lambda x: '.csv' in x, os.listdir(ESTABELECIMENTOSCSV)))

os.system('cls')
# Aplica a função Extração de empresas no palalelismo
Parallel(n_jobs=5,
	backend='threading', # multiprocessing
	verbose=10,
	batch_size=3,
	prefer='threads',
	pre_dispatch='1 * n_jobs')(delayed(Extracao_CNAE)(file,ESTABELECIMENTOSCSV) for file in all_files_ESTABELECIMENTOSCSV)

# alternativazinha de quebra mas o normal é ficar tudo normal.

"""

Extracao_CNAE(diretorio=diretorio,file=all_files[0])
Extracao_CNAE(diretorio=diretorio,file=all_files[1])
Extracao_CNAE(diretorio=diretorio,file=all_files[2])
Extracao_CNAE(diretorio=diretorio,file=all_files[3])
Extracao_CNAE(diretorio=diretorio,file=all_files[4])
Extracao_CNAE(diretorio=diretorio,file=all_files[5])
Extracao_CNAE(diretorio=diretorio,file=all_files[6])
Extracao_CNAE(diretorio=diretorio,file=all_files[7])
Extracao_CNAE(diretorio=diretorio,file=all_files[8])
Extracao_CNAE(diretorio=diretorio,file=all_files[9])

"""