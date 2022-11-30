# ler todos os arquivos csv do diretório e guardar em um objeto
# import required modules
import numpy as np
import time
import os
import re
import pandas as pd
from dask import dataframe as dd

diretorio = r'Bases/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

#Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings
warnings.filterwarnings("ignore")

#Comando para exibir todas colunas do arquivo
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

for file in all_files:
    print(f'Operando arquivo {file}')
    
    with open(f"{diretorio}/{file}", mode='r', encoding='utf-8', errors='ignore') as arq:
        linhas = len(arq.readlines())
        arq.close()

    print(linhas)
