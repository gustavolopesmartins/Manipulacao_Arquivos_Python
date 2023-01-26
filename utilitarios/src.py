# ler todos os arquivos csv do diretório e guardar em um objeto
# import required modules
import numpy as np
import time
import pandas as pd
from dask import dataframe as dd
import csv
import os

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
dtypes = {'CNPJ_BASE': 'category',
 'CNPJ_ORDEM': 'category',
 'CNPJ_DV': 'category'}
dtypes1 = { 'MATRIZ_FILIAL': 'category',
 'NOME_FANTASIA': 'category',
 'SITUACAO_CADASTRAL': 'int32',
 'DATA_SITUACAO_CADASTRAL': 'int32',
 'MOTIVO_SITUACAO_CADASTRAL': 'category',
 'CIDADE_EXTERIOR': 'category',
 'PAIS': 'category',
 'DATA_INICIO_ATIVIDADE': 'category',
 'CNAE_PRINCIPAL': 'int32',
 'CNAE_SECUNDARIO': 'category',
 'TIPO_LOGRADOURO': 'category',
 'LOGRADOURO': 'category',
 'NUMERO': 'category',
 'COMPLEMENTO': 'category',
 'BAIRRO': 'category',
 'CEP': 'str',
 'UF': 'category',
 'MUNICIPIO': 'str',
 'DDD1': 'str',
 'TELEFONE1': 'str',
 'DDD2': 'str',
 'TELEFONE2': 'str',
 'DDD_FAX': 'str',
 'FAX': 'str',
 'EMAIL': 'category',
 'SITUACAO_ESPECIAL': 'category',
 'DATA_SITUACAO_ESPECIAL': 'str'}

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
    dados = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'],usecols=colunas, nrows=int(n_linhas)-1, dtype=dtypes1, na_values=['non-numeric value'],converters={'CNAE_SECUNDARIO':str},encoding_errors='ignore')
    dados['NOME_FANTASIA'].fillna('null', inplace=True)
    dados.drop_duplicates(inplace=True)
    indice_remove = dados[(dados['SITUACAO_CADASTRAL'] != 2) & (dados['SITUACAO_CADASTRAL'] != 3) &(dados['SITUACAO_CADASTRAL'] != 4)& (dados['SITUACAO_CADASTRAL'] != 5)].index
    dados.drop(indice_remove, inplace=True) 
    
    for cnae in lista_cnae:
        
        globals()[f'df_{cnae}'] = dados.loc[dados['CNAE_PRINCIPAL']== cnae]
        globals()[f'df_{cnae}'].to_csv(f'Bases/{CNAES[cnae]}.csv', mode='a', index=False, sep=';', encoding='utf-8',header=False)
        del globals()[f'df_{cnae}'] #= pd.DataFrame()
        #pd.concat(globals()[f'df_{cnae}'],dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
        #globals()[f'df_{cnae}'].append(dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
    
    # Desmontando o DataFrame
    del dados #= pd.DataFrame()
    # Contando a leitura dos arquivos para não ficar pesado demais


    # Montando o último DataFrame do arquivo usado
    dados = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'],usecols=colunas,skiprows=int(pulo), nrows=int(n_linhas)-1, dtype=dtypes1, na_values=['non-numeric value'],converters={'CNAE_SECUNDARIO':str},encoding_errors='ignore')
    #dados = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'],usecols=colunas,skiprows=int(pulo), nrows=int(n_linhas),dtype=dtypes1)
    dados['NOME_FANTASIA'].fillna('null', inplace=True)
    dados.drop_duplicates(inplace=True)
    indice_remove = dados[(dados['SITUACAO_CADASTRAL'] != 2) & (dados['SITUACAO_CADASTRAL'] != 3) &(dados['SITUACAO_CADASTRAL'] != 4)& (dados['SITUACAO_CADASTRAL'] != 5)].index
    dados.drop(indice_remove, inplace=True)
    
    for cnae in lista_cnae:
        globals()[f'df_{cnae}'] = dados.loc[dados['CNAE_PRINCIPAL']== cnae]
        globals()[f'df_{cnae}'].to_csv(f'Bases/{CNAES[cnae]}.csv', mode='a', index=False,sep=';', encoding='utf-8', header=False)
        del globals()[f'df_{cnae}'] #= pd.DataFrame()
        #globals()[f'df_{cnae}'].append(dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
        #pd.concat(globals()[f'df_{cnae}'],dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
    # finalizando o cronômetro do processo
    fim = time.time()
    
    # Desmontando o último DataFrame
    del dados #= pd.DataFrame()
    retorno = f'Lidos no arquivo {file} o total de {linhas} linhas em {(fim-inicio)} segundos'
    return print(retorno)


def Extracao_EMPRE(file:str = None, diretorio:str = r'./'):
    files = file.split('.')
    file_name = files[1]
    dtypes_EMPRE = {'CNPJ_BASE': 'category'}
    inicio = time.time()
    print(f'Operando arquivo {file}')
    
    with open(f"{diretorio}/{file}", mode='r', encoding='ISO-8859-1', errors='ignore') as arq:
        linhas = len(arq.readlines())
        arq.close()
    
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
    #dados = dd.from_pandas(pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'], dtype=dtypes, nrows=int(n_linhas)-1), npartitions=10)
    dados1 = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['EMPRE'],usecols=[0 , 1], nrows=int(n_linhas)-1, dtype=dtypes_EMPRE)
    dados1.drop_duplicates(inplace=True)
    


    dados1.to_csv(f'Bases_EMPRESAS/LISTA_EMPRESAS{file_name}.csv', mode='a', index=False, sep=';', encoding='utf-8',header=False)
        

    del dados1
    # Montando o último DataFrame do arquivo usado
    #dados = dd.from_pandas(pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'],skiprows=int(pulo), nrows=int(n_linhas)), npartitions=10)
    dados2 = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['EMPRE'],usecols=[0 , 1],skiprows=int(pulo), nrows=int(n_linhas),dtype=dtypes_EMPRE)

    dados2.drop_duplicates(inplace=True)

    dados2.to_csv(f'Bases_EMPRESAS/LISTA_EMPRESAS{file_name}.csv', mode='a', index=False, sep=';', encoding='utf-8',header=False)
    # finalizando o cronômetro do processo
    fim = time.time()
    
    # Desmontando o último DataFrame
    del dados2 #= pd.DataFrame()
    retorno = f'Lidos no arquivo {file} o total de {linhas} linhas em {(fim-inicio)} segundos'
    return print(retorno)