import os
import csv
import time
from src import Extracao_CNAE
diretorio = r'C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))
diretorio_destino = r'C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSZIP'

os.system('cls')
#from descompactador import descompactador
#descompactador(diretorioatual=diretorio, diretoriodestino=diretorio_destino)
#from convertearquivo import convertearquivo
#convertearquivo(diretorio=diretorio_destino, tipo_atual='EMPRECSV',tipo_destino='csv')

Extracao_CNAE(diretorio=diretorio,file=all_files[0])
Extracao_CNAE(diretorio=diretorio,file=all_files[1])
Extracao_CNAE(diretorio=diretorio,file=all_files[2])
Extracao_CNAE(diretorio=diretorio,file=all_files[3])
#Extracao_CNAE(diretorio=diretorio,file=all_files[4])
#Extracao_CNAE(diretorio=diretorio,file=all_files[5])
#Extracao_CNAE(diretorio=diretorio,file=all_files[6])
#Extracao_CNAE(diretorio=diretorio,file=all_files[7])
#Extracao_CNAE(diretorio=diretorio,file=all_files[8])
#Extracao_CNAE(diretorio=diretorio,file=all_files[9])
