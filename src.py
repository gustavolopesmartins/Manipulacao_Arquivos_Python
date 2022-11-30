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
              'DATA_SITUACAO_CADASTRAL'  , 'MOTIVO_SITUACAO_CADASTRAL'  , 'CIDADE_EXTERIOR'  , 'PAIS'  , 'DATA_INICIO_ATIVIDADE'  ,
              'CNAE_PRINCIPAL'  , 'CNAE_SECUNDARIO' , 'TIPO_LOGRADOURO'  , 'LOGRADOURO'  , 'NUMERO'  , 'COMPLEMENTO' ,
              'BAIRRO'  , 'CEP'  , 'UF'  , 'MUNICIPIO'  , 'DDD1'  , 'TELEFONE1'  , 'DDD2'  , 'TELEFONE2'  ,
              'DDD_FAX'  , 'FAX'  , 'EMAIL'  , 'SITUACAO_ESPECIAL'  , 'DATA_SITUACAO_ESPECIAL'],  

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
dtypes = {'CNPJ_BASE': 'category',
 'CNPJ_ORDEM': 'category',
 'CNPJ_DV': 'category',
 'MATRIZ_FILIAL': 'category',
 'NOME_FANTASIA': 'category',
 'SITUACAO_CADASTRAL': 'float32',
 'DATA_SITUACAO_CADASTRAL': 'float32',
 'MOTIVO_SITUACAO_CADASTRAL': 'category',
 'CIDADE_EXTERIOR': 'float32',
 'PAIS': 'float32',
 'DATA_INICIO_ATIVIDADE': 'float32',
 'CNAE_PRINCIPAL': 'float32',
 'CNAE_SECUNDARIO': 'float32',
 'TIPO_LOGRADOURO': 'float32',
 'LOGRADOURO': 'category',
 'NUMERO': 'float32',
 'COMPLEMENTO': 'float32',
 'BAIRRO': 'float32',
 'CEP': 'float32',
 'UF': 'float32',
 'MUNICIPIO': 'float32',
 'DDD1': 'float32',
 'TELEFONE1': 'float32',
 'DDD2': 'float32',
 'TELEFONE2': 'float32',
 'DDD_FAX': 'float32',
 'FAX': 'float32',
 'EMAIL': 'float32',
 'SITUACAO_ESPECIAL': 'float32',
 'DATA_SITUACAO_ESPECIAL': 'float32'}

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
    dados = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'], nrows=int(n_linhas)-1)
    
    for cnae in lista_cnae:
        
        globals()[f'df_{cnae}'] = dados.loc[dados['CNAE_PRINCIPAL']== cnae]
        globals()[f'df_{cnae}'].to_csv(f'Bases/{CNAES[cnae]}.csv', mode='a', index=False, sep=';', encoding='utf-8')
        del globals()[f'df_{cnae}'] #= pd.DataFrame()
        time.sleep(3)
        globals()[f'df_{cnae}'] = dados.loc[dados['CNAE_SECUNDARIO']== cnae]
        globals()[f'df_{cnae}'].to_csv(f'Bases/{CNAES[cnae]}.csv', mode='a', index=False, sep=';', encoding='utf-8', header=False)
        del globals()[f'df_{cnae}'] #= pd.DataFrame()
        time.sleep(3)
        #pd.concat(globals()[f'df_{cnae}'],dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
        #globals()[f'df_{cnae}'].append(dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
    
    # Desmontando o DataFrame
    del dados #= pd.DataFrame()
    # Contando a leitura dos arquivos para não ficar pesado demais

    time.sleep(3)

    # Montando o último DataFrame do arquivo usado
    #dados = dd.from_pandas(pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'],skiprows=int(pulo), nrows=int(n_linhas)), npartitions=10)
    dados = pd.read_csv(f'{diretorio}/{file}',sep=';',encoding='ISO-8859-1', names=CNPJ['ESTABELE'],skiprows=int(pulo), nrows=int(n_linhas))
    
    for cnae in lista_cnae:
        globals()[f'df_{cnae}'] = dados.loc[dados['CNAE_PRINCIPAL']== cnae]
        globals()[f'df_{cnae}'].to_csv(f'Bases/{CNAES[cnae]}.csv', mode='a', index=False,sep=';', encoding='utf-8', header=False)
        del globals()[f'df_{cnae}'] #= pd.DataFrame()
        time.sleep(3)
        globals()[f'df_{cnae}'] = dados.loc[dados['CNAE_SECUNDARIO']== cnae]
        globals()[f'df_{cnae}'].to_csv(f'Bases/{CNAES[cnae]}.csv', mode='a', index=False, sep=';', encoding='utf-8',header=False)
        del globals()[f'df_{cnae}'] #= pd.DataFrame()
        #globals()[f'df_{cnae}'].append(dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
        #pd.concat(globals()[f'df_{cnae}'],dados.loc[dados['CNAE_PRINCIPAL']== cnae], ignore_index=True)
        time.sleep(3)
    # finalizando o cronômetro do processo
    fim = time.time()
    
    # Desmontando o último DataFrame
    del dados #= pd.DataFrame()
    time.sleep(1)
    retorno = f'Lidos no arquivo {file} o total de {linhas} linhas em {(fim-inicio)} segundos'
    return print(retorno)


