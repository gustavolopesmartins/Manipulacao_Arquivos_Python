import os
import csv
import time
from src import Extracao_CNAE
diretorio = r'C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

os.system('cls')

for file in all_files:
    try:
        Extracao_CNAE(diretorio=diretorio,file=file)
    except:
        time.sleep(10)
        Extracao_CNAE(diretorio=diretorio,file=file)
#Extracao_CNAE(diretorio=diretorio, file= file)
