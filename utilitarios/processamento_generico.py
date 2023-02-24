import os
from joblib import Parallel, delayed
from src import Extracao_CNAE

ESTABELECIMENTOSCSV =r"C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV"


# Filtra todos os arquivos csv da pasta após o processo de descompactação e conversão aplicados
all_files_ESTABELECIMENTOSCSV = list(filter(lambda x: '.csv' in x, os.listdir(ESTABELECIMENTOSCSV)))

os.system('cls')
# Aplica a função Extração de empresas no palalelismo
Parallel(n_jobs=1,
	backend='loky', # multiprocessing
	verbose=10,
	batch_size=3,
	prefer='processes',
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