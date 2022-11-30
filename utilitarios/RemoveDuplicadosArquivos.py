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
    ffile =re.sub(" ","_",file)
    globals()[f'{ffile}'] = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='utf-8',low_memory=False)
    print(f'ANTES \nNome: {file} Tamanho: {globals()[f"{ffile}"].shape[0]}')
    globals()[f'{ffile}'].drop_duplicates(inplace=True)
    globals()[f'{ffile}'].to_csv(f'Bases/Atualizado_{file}', index=False, sep=';', encoding='utf-8', header=False)
    print(f'DEPOIS \nNome: {file} Tamanho: {globals()[f"{ffile}"].shape[0]}')
