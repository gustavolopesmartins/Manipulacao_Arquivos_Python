import os
import csv
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from joblib import Parallel, delayed
from src import Extracao_CNAE
#diretorio = r"C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSZIP"
diretorio =r"C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV"
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

os.system('cls')
#from descompactador import descompactador
#descompactador(diretorioatual=diretorio, diretoriodestino=diretorio_destino)
#from convertearquivo import convertearquivo
#convertearquivo(diretorio=diretorio_destino, tipo_atual='ESTABELE',tipo_destino='csv')

"""
Parallel(n_jobs=3,
        backend='threading', # multiprocessing
        verbose=10,
        batch_size=3,
        prefer='threads',
        pre_dispatch='1 * n_jobs')(delayed(Extracao_CNAE)(file,diretorio) for file in all_files)
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
