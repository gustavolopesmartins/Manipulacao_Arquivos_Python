import os
import timeit
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
file_log = current_dir + r"/logs/src.log"
# gerando log
logging.basicConfig(level=logging.DEBUG, filename=file_log, format="%(asctime)s - %(levelname)s - %(message)s")

def convertearquivo(diretorio:str = os.getcwd(),tipo_atual:str = '.', tipo_destino:str = '.' ):
    """
        Busca em um diretório a extenção de arquivo específica e converte para a sugerida

        Parâmetros:
            diretorio : Caminho onde está os arquivos a serem editados
            tipo_atual : Extensão do arquivo exemplo : '.ESTABELE'
            tipo_destino : Extensão do arquivo exemplo : '.csv'
        
        Padrão
            os.getcwd() : Diretório Atual
            '.' : (pega todos os arquivo e não converte nada - pode corromper seu arquivo fica a dica)

        Libs Utilizadas
        
    """
    import re
    os.system('cls')

    #diretorioatual = os.getcwd()
    #diretorio = f'{diretorioatual}/ESTABELECIMENTOSCSV'
    #tipo_atual = '.ESTABELE'
    #tipo_destino = '.csv'

    files = list(filter(lambda x: tipo_atual in x, os.listdir(f"{diretorio}/")))


    for file in files:
        # Selecionando Base para captura de Layout e nome do Arquivo
        #print(file)
        camada0 = file.split('.')
        #print(camada0)
        camada1 = camada0[-1]
        #print(camada1)
        new_file = re.sub(camada1,tipo_destino,file)
        #print(new_file)
        os.rename(f'{diretorio}/{file}', f'{diretorio}/{new_file}')
        #file.replace('ESTABELE', '').split('.')[-1].replace('CSV', '') if file.find('ESTABELE') < 0 else 'SIMPLES'
    tempo_gasto = timeit.timeit(stmt='a=10;b=10;sum=a+b')
    logging.info(f"Processo de conversão concluído, tempo gasto: {tempo_gasto}")