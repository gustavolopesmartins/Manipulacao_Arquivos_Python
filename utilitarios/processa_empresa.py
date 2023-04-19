import os
from joblib import Parallel, delayed
from src import Extracao_EMPRE
from backup_limpeza import backup_limpeza_simples
from descompactador import descompactador
from convertearquivo import convertearquivo
from datetime import datetime

# definindo os diretórios de quebradinha.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
EMPRESASZIP = CURRENT_DIR.replace(r'Manipulacao_arquivos\Manipulacao_Arquivos_Python\utilitarios',r'EMPRESASZIP' )
EMPRESASCSV = CURRENT_DIR.replace(r'Manipulacao_arquivos\Manipulacao_Arquivos_Python\utilitarios',r'EMPRESASCSV')
BASES_EMPRESAS = CURRENT_DIR.replace(r'utilitarios', r'Bases_EMPRESAS')

# difinindo nome para arquivo zipado
datazip = f'{datetime.now().day}-{datetime.now().month}-{datetime.now().year}'
nome_zip_empresas_csv = f'{EMPRESASCSV}/periodos_anteriores/{datazip}_EMPRESAS.zip'

# Filtra todos os arquivos csv da pasta
all_files_Bases_EMPRESAS = list(filter(lambda x: '.csv' in x, os.listdir(BASES_EMPRESAS)))
# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(all_files_Bases_EMPRESAS) >= 1:
        backup_limpeza_simples(pasta=BASES_EMPRESAS, nome_zipado=f'{BASES_EMPRESAS}/{datazip}_Bases_EMPRESAS.zip')
# Filtra todos os arquivos csv da pasta
all_files_EMPRESASCSV = list(filter(lambda x: '.csv' in x, os.listdir(EMPRESASCSV)))
# Se a pasta não estiver vazia faz o backup dos arquivos e limpa ela.
if len(all_files_EMPRESASCSV) >= 1:
    backup_limpeza_simples(pasta=EMPRESASCSV, nome_zipado=nome_zip_empresas_csv)

# descompacta os arquivos em zip antes de extrair as empresas
descompactador(diretorioatual=EMPRESASZIP, diretoriodestino=EMPRESASCSV)
convertearquivo(diretorio=EMPRESASCSV,tipo_atual='EMPRECSV', tipo_destino='csv')

# Filtra todos os arquivos csv da pasta após o processo de descompactação e conversão aplicados
all_files_EMPRESASCSV = list(filter(lambda x: '.csv' in x, os.listdir(EMPRESASCSV)))

os.system('cls')
# Aplica a função Extração de empresas no palalelismo
Parallel(n_jobs=5,
	backend='threading', # multiprocessing
	verbose=10,
	batch_size=3,
	prefer='threads',
	pre_dispatch='1 * n_jobs')(delayed(Extracao_EMPRE)(file,EMPRESASCSV) for file in all_files_EMPRESASCSV)


"""
def processo(arquivo):
    Extracao_EMPRE(file=arquivo, diretorio=ESTABELECIMENTOSCSV)
 
#if __name__ == '__main__':
with multiprocessing.Pool(processes=5) as pool:
	pool.map(processo, all_files_EMPRESASCSV)
"""