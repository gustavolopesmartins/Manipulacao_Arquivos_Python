import os
import csv
import time
from src import Extracao_CNAE
diretorio = r'C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV/'
all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

os.system('cls')

#Extracao_CNAE(diretorio=diretorio,file='K3241.K03200Y0.D21119.csv')
#Extracao_CNAE(diretorio=diretorio,file='K3241.K03200Y2.D21119.csv')
#Extracao_CNAE(diretorio=diretorio,file='K3241.K03200Y3.D21119.csv')
#Extracao_CNAE(diretorio=diretorio,file='K3241.K03200Y4.D21119.csv')
#Extracao_CNAE(diretorio=diretorio,file='K3241.K03200Y6.D21119.csv')
#Extracao_CNAE(diretorio=diretorio,file='K3241.K03200Y7.D21119.csv')
#Extracao_CNAE(diretorio=diretorio,file='K3241.K03200Y8.D21119.csv')
#Extracao_CNAE(diretorio=diretorio,file="K3241.K03200Y9.D21119.csv")
