# ler todos os arquivos csv do diretório e guardar em um objeto
# import required modules
import numpy as np
import time
import pandas as pd
from dask import dataframe as dd
import csv
import os
import logging

# gerando log
logging.basicConfig(level=logging.INFO, filename="src.log", format="%(asctime)s - %(levelname)s - %(message)s")

#diretorio = r'C:\Users\ABRASEL NACIONAL\Documents\CNPJ_PROGRAMATICA\ESTABELECIMENTOSCSV/'
#all_files = list(filter(lambda x: '.csv' in x, os.listdir(diretorio)))

# Warnings: Possui uma série de funções e comandos para tratamento de mensagens de avisos e alertas do Python
import warnings
warnings.filterwarnings("ignore")

# Iniciando o cronômetro do processo
inicio = time.time()

CNPJ = {"EMPRE" : ['CNPJ_BASE', 'RAZAO_SOCIAL', 'NATUREZA_JURIDICA' , 'QUALIFICACAO' ,
          'VALOR_CAPITAL_SOCIAL' , 'PORTE_EMPRESA'  , 'ENTE_FEDERATIVO'],

"ESTABELE" : ['CNPJ_BASE', 'CNPJ_ORDEM' , 'CNPJ_DV' , 'MATRIZ_FILIAL'  , 'NOME_FANTASIA' , 'SITUACAO_CADASTRAL' ,
              'DATA_SITUACAO_CADASTRAL'  , 'MOTIVO_SITUACAO_CADASTRAL'  , 'DATA_INICIO_ATIVIDADE'  ,
              'CNAE_PRINCIPAL'  , 'CNAE_SECUNDARIO' , 'TIPO_LOGRADOURO'  , 'LOGRADOURO'  , 'NUMERO'  , 'COMPLEMENTO' ,
              'BAIRRO'  , 'CEP'  , 'UF'  , 'MUNICIPIO'  , 'DDD1'  , 'TELEFONE1'  , 'DDD2'  , 'TELEFONE2'  ,
              'DDD_FAX'  , 'FAX'  , 'EMAIL'],  

"SIMPLES" : ['CNPJ_BASE'  , 'OPCAO_SIMPLES'  , 'DATA_OPCAO_SIMPLES'  , 'DATA_EXCLUSAO_SIMPLES'  ,
             'OPCAO_MEI'  , 'DATA_OPCAO_MEI'  , 'DATA_EXCLUSAO_MEI'],

"SOCIO" : ['CNPJ_BASE'  , 'TIPO'  , 'NOME'  , 'CPF_CNPJ'  , 'QUALIFICACAO'  , 'DATA_ENTRADA' ,
            'PAIS' , 'REPRESENTANTE'  , 'NOME_REPRESENTANTE'  , 'QUALIFICACAO_REPRESENTANTE'  , 'FAIXA_ETARIA'  ],

"PAIS" : ['CODIGO_PAIS' , 'PAIS'],

"MUNIC" : ['MUNICIPIO'  , 'MUNICIPIO'  ],

"QUALS" : ['QUALIFICACAO', 'QUALIFICACAO' ],

"NATJU" : ['NATUREZA_JURIDICA'  , 'NATUREZA_JURIDICA'],

"MOTI" : ['MOTIVO_SITUACAO_CADASTRAL'  , 'MOTIVO_SITUACAO_CADASTRAL'  ],

"CNAE" : ['CODIGO_CNAE'  , 'CNAE' ]}
colunas = [0,1,2,3,4,5,6,7,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]
dtypes = { 
 'CNPJ_BASE': 'category',
 'CNPJ_ORDEM': 'category' ,
 'CNPJ_DV': 'category',
 'MATRIZ_FILIAL': 'object',
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
 'CEP': 'str',
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
 'DATA_SITUACAO_ESPECIAL': 'object'
 }

"""CNAES = {5510801: 'Hotéis',
5510802: 'Apart hotéis',
5510803: 'Motéis',
5590601: 'Albergues, exceto assistenciais',
5590602: 'Campings',
5590603: 'Pensões(alojamento)',
5590699: 'Outros alojamentos não especificados anteriormente'}"""

CNAES = {
        5612100:'Serviços ambulantes de alimentação',
        5611201:'Restaurantes e similares',
        5611203:'Lanchonetes casas de chá de sucos e similares',
        5611204:'Bares e outros estabelecimentos especializados em servir bebidas sem entretenimento',
        5611205:'Bares e outros estabelecimentos especializados em servir bebidas com entretenimento'
        }

lista_cnae = []
for cnae in CNAES.keys():
    lista_cnae.append(cnae)


total_dados = {'5612100' : [],'5611201' : [],'5611203' : [], '5611204': [], '5611205': [] , 'arquivo': []}

def Extracao_CNAE(file:str = None, diretorio:str = r'./'):
    inicio = time.time()
    #print(f'Operando arquivo {file}')
    logging.info(f'Operando arquivo {file}')
    linhas = 0
    with open(f"{diretorio}/{file}", mode='r', encoding='ISO-8859-1', errors='ignore') as arq:
        for linha in arq:
            linhas += 1
        #print(linhas)
    
    # Montando os DataFrames
    chunk_dados = pd.read_csv(f'{diretorio}/{file}',
    sep=';',
    encoding='ISO-8859-1', 
    names=CNPJ['ESTABELE'],
    usecols=colunas, 
    nrows=int(linhas)-1, 
    dtype=dtypes, 
    na_values=['non-numeric value'],
    converters={'CNAE_SECUNDARIO':str},
    encoding_errors='ignore', chunksize=1000000)
    for dados in chunk_dados:
        dados.header = CNPJ['ESTABELE']
        print(f'Leitura inicial: {len(dados)}')
        dados['NOME_FANTASIA'].fillna('--empty--', inplace=True)
        #print(dados['SITUACAO_CADASTRAL'].value_counts())
        #dados.drop_duplicates(inplace=True)
        #print(f'Após remover duplicados: {len(dados)}')
        dados = dados[(dados['SITUACAO_CADASTRAL'] == '02') | (dados['SITUACAO_CADASTRAL'] == '05')].loc[:,].reset_index(drop=True)
        print(f'Somente os ativos: {len(dados)}')
        
        for cnae in lista_cnae:
            # Separando o dataframe com base nos códigos CNAEs
            globals()[f'df_{cnae}'] =  dados.loc[dados['CNAE_PRINCIPAL'] == cnae]
            # Concatenando os dados de Telefone e Fax
            globals()[f'df_{cnae}']['TELEFONE1'] = "+55" + globals()[f'df_{cnae}']['DDD1'].map(str) + globals()[f'df_{cnae}']['TELEFONE1'].map(str)
            globals()[f'df_{cnae}']['TELEFONE2'] = "+55" + globals()[f'df_{cnae}']['DDD2'].map(str) + globals()[f'df_{cnae}']['TELEFONE2'].map(str)
            globals()[f'df_{cnae}']['FAX'] = "+55" + globals()[f'df_{cnae}']['DDD_FAX'].map(str) + globals()[f'df_{cnae}']['FAX'].map(str)
            # Dropando os DDDs após a concatenação
            globals()[f'df_{cnae}'].drop(['DDD1','DDD2','DDD_FAX'], axis=1, inplace=True)
            # Mudando o tipo de dado da coluna 'CEP' para string
            globals()[f'df_{cnae}']['CEP'] = globals()[f'df_{cnae}']['CEP'].astype(str)
            # Adicionando hífen à coluna 'CEP'
            globals()[f'df_{cnae}']['CEP'] = globals()[f'df_{cnae}']['CEP'].str[:5] + '-' + globals()[f'df_{cnae}']['CEP'].str[5:]
            # Mapeando colunas do DataFrame no log
            logging.info(f"Colunas Geradas: {globals()[f'df_{cnae}'].columns}")
            # Contando o número de itens por DataFrames exportados
            logging.info(f"Itens capiturados: {len(globals()[f'df_{cnae}'])} Categoria dos dados: {CNAES[cnae]}")
            total_dados[str(cnae)].append(len(globals()[f'df_{cnae}']))
            
            # Exporta como CSV
            globals()[f'df_{cnae}'].to_csv(f'../Bases/{CNAES[cnae]}.csv', mode='a', index=False, sep=';', encoding='utf-8',header=False)
        total_dados['arquivo'].append(file)
        dados_2 = pd.DataFrame(total_dados)
        dados_2.to_csv('../Bases/Dados.csv',index=False,sep=';', encoding='utf-8')
    # Finaliza o cronômetro
    fim = time.time()
    retorno = f'Lidos no arquivo {file} o total de {linhas} linhas em {(fim-inicio)} segundos'
    #print(retorno)
    logging.info(retorno)

def Extracao_EMPRE(file:str = None, diretorio:str = r'./'):
    files = file.split('.')
    file_name = files[1]
    dtypes_EMPRE = {'CNPJ_BASE': 'category'}
    print(f'Operando arquivo {file}')
    inicio = time.time()
    #print(f'Operando arquivo {file}')
    logging.info(f'Operando arquivo {file}')
    linhas = 0
    with open(f"{diretorio}/{file}", mode='r', encoding='ISO-8859-1', errors='ignore') as arq:
        for linha in arq:
            linhas += 1
        #print(linhas)
    
    # Montando os DataFrames
    chunk_dados = pd.read_csv(f'{diretorio}/{file}',
    sep=';',
    encoding='ISO-8859-1', 
    names=CNPJ['EMPRE'],
    usecols=[0 , 1], 
    nrows=int(linhas), 
    dtype=dtypes_EMPRE, 
    encoding_errors='ignore', 
    chunksize=1000000)    
    
    for dados in chunk_dados:
        dados.header = CNPJ['EMPRE']
        print(f'Leitura inicial: {len(dados)}')
        dados.drop_duplicates(inplace=True)
        #print(dados['SITUACAO_CADASTRAL'].value_counts())
        #dados.drop_duplicates(inplace=True)
        #print(f'Após remover duplicados: {len(dados)}')    
    

    # Levando pra fora os dados
    dados.to_csv(f'../Bases_EMPRESAS/LISTA_EMPRESAS_{file_name}.csv', mode='a', index=False, sep=';', encoding='utf-8')

    # finalizando o cronômetro do processo
    fim = time.time()

    retorno = f'Lidos no arquivo {file} o total de {linhas} linhas em {(fim-inicio)} segundos'
    print(retorno)