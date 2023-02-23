import os
import csv
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from joblib import Parallel, delayed
from src import Extracao_EMPRE
from backup_limpeza import backup_limpeza_simples
from descompactador import descompactador

# definindo os diretórios de quebradinha.
EMPRESASZIP = r"C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\EMPRESASZIP"
EMPRESASCSV =r"C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\EMPRESASCSV"
Bases_EMPRESAS = r'../Bases_EMPRESAS/'

# Filtra todos os arquivos csv da pasta
all_files_Bases_EMPRESAS = list(filter(lambda x: '.csv' in x, os.listdir(Bases_EMPRESAS)))
# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(all_files_Bases_EMPRESAS) >= 1:
        backup_limpeza_simples(pasta=Bases_EMPRESAS, nome_zipado=f'{Bases_EMPRESAS}Bases_EMPRESAS_01-2023.zip')
# Filtra todos os arquivos csv da pasta
all_files_EMPRESASCSV = list(filter(lambda x: '.csv' in x, os.listdir(EMPRESASCSV)))
# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(all_files_EMPRESASCSV) >= 1:
    backup_limpeza_simples(pasta=EMPRESASCSV, nome_zipado=f'{EMPRESASCSV}/periodos anteriores/2023_jan_EMPRESASCSV.zip')

# descompacta os arquivos em zip antes de extrair as empresas
descompactador(diretorioatual=EMPRESASZIP, diretoriodestino=EMPRESASCSV)

os.system('cls')
# Aplica a função Extração de empresas no palalelismo
Parallel(n_jobs=5,
	backend='threading', # multiprocessing
	verbose=10,
	batch_size=3,
	prefer='threads',
	pre_dispatch='1 * n_jobs')(delayed(Extracao_EMPRE)(file,EMPRESASCSV) for file in all_files_EMPRESASCSV)

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