# ler todos os arquivos csv do diretório e guardar em um objeto
# import required modules
import numpy as np
import time
import pandas as pd
import dask.dataframe as dd
import csv
import os
import re
import logging

# gerando log
logging.basicConfig(level=logging.INFO, filename="src.log", format="%(asctime)s - %(levelname)s - %(message)s")

# Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings
warnings.filterwarnings("ignore")

# Iniciando o cronômetro do processo
inicio = time.time()

ESTABELE =['CNPJ_BASE', 'CNPJ_ORDEM' , 'CNPJ_DV' , 'MATRIZ_FILIAL'  , 'NOME_FANTASIA' , 'SITUACAO_CADASTRAL' ,
              'DATA_SITUACAO_CADASTRAL'  , 'MOTIVO_SITUACAO_CADASTRAL'  , 'DATA_INICIO_ATIVIDADE'  ,
              'CNAE_PRINCIPAL'  , 'CNAE_SECUNDARIO' , 'TIPO_LOGRADOURO'  , 'LOGRADOURO'  , 'NUMERO'  , 'COMPLEMENTO' ,
              'BAIRRO'  , 'CEP'  , 'UF'  , 'MUNICIPIO'  , 'DDD1'  , 'TELEFONE1'  , 'DDD2'  , 'TELEFONE2'  ,
              'DDD_FAX'  , 'FAX'  , 'EMAIL']
colunas = [0,1,2,3,4,5,6,7,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]
dtypes1 = { 'MATRIZ_FILIAL': 'object',
 'NOME_FANTASIA': 'object',
 'SITUACAO_CADASTRAL': 'object',
 'DATA_SITUACAO_CADASTRAL': 'object',
 'MOTIVO_SITUACAO_CADASTRAL': 'object',
 'CIDADE_EXTERIOR': 'object',
 'PAIS': 'object',
 'DATA_INICIO_ATIVIDADE': 'object',
 'CNAE_PRINCIPAL': 'int32',
 'CNAE_SECUNDARIO': 'object',
 'TIPO_LOGRADOURO': 'object',
 'LOGRADOURO': 'object',
 'NUMERO': 'object',
 'COMPLEMENTO': 'object',
 'BAIRRO': 'object',
 'CEP': 'object',
 'UF': 'object',
 'MUNICIPIO': 'object',
 'DDD1': 'object',
 'TELEFONE1': 'object',
 'DDD2': 'object',
 'TELEFONE2': 'object',
 'DDD_FAX': 'object',
 'FAX': 'object',
 'EMAIL': 'object',
 'SITUACAO_ESPECIAL': 'int32',
 'DATA_SITUACAO_ESPECIAL': 'object'}

"""CNAES = {5510801: 'Hotéis',
5510802: 'Apart hotéis',
5510803: 'Motéis',
5590601: 'Albergues, exceto assistenciais',
5590602: 'Campings',
5590603: 'Pensões(alojamento)',
5590699: 'Outros alojamentos não especificados anteriormente'}"""

CNAES = {5611201:'Restaurantes e similares',
        5611203:'Lanchonetes casas de chá de sucos e similares',
        5611204:'Bares e outros estabelecimentos especializados em servir bebidas sem entretenimento',
        5611205:'Bares e outros estabelecimentos especializados em servir bebidas com entretenimento',
        5612100:'Serviços ambulantes de alimentação'}

lista_cnae = []
for cnae in CNAES.keys():
    lista_cnae.append(cnae)



def Extracao_CNAE(file:str = None, diretorio:str = r'./'):
    inicio = time.time()
    print(f'Operando arquivo {file}')
    logging.info(f'Operando arquivo {file}')
    linhas = 0
    with open(f"{diretorio}/{file}", mode='r', encoding='ISO-8859-1', errors='ignore') as arq:
        for linha in arq:
            linhas += 1
    if (linhas%2) == 0:
        pulo = linhas / 2
        n_linhas = linhas / 2
        print(linhas)
    else:
        pulo = (linhas - 1) / 2
        n_linhas = (linhas + 1) / 2
        print(linhas)
    
    time.sleep(5)
    # Montando os DataFrames
    chunk_dados = pd.read_csv(f'{diretorio}/{file}',
    sep=';',
    encoding='ISO-8859-1', 
    names=ESTABELE,
    usecols=colunas, 
    nrows=int(n_linhas)-1, 
    dtype=dtypes1, 
    na_values=['non-numeric value'],
    converters={'CNAE_SECUNDARIO':str},
    encoding_errors='ignore', chunksize=1000000)
    for dados in chunk_dados:
        
        if header is None:
            header = dados.columns
            continue
        dados.columns = header
        print(header)
        print(len(dados))
        print(dados.columns)
        dados['NOME_FANTASIA'].fillna('--empty--', inplace=True)
        dados.drop_duplicates(inplace=True)
        dados = dados[(dados['SITUACAO_CADASTRAL'] == 2)].index
        

        for cnae in lista_cnae:
            globals()[f'df_{cnae}'] = dados.loc[dados['CNAE_PRINCIPAL']== cnae]
            globals()[f'df_{cnae}'].to_csv(f'Bases/{CNAES[cnae]}.csv', mode='a', index=False, sep=';', encoding='utf-8',header=False)
            del globals()[f'df_{cnae}'] #= pd.DataFrame()
    fim = time.time()
    retorno = f'Lidos no arquivo {file} o total de {linhas} linhas em {(fim-inicio)} segundos'
    print(retorno)
    logging.info(retorno)