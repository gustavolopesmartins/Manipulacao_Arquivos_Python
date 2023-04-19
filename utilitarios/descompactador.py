import os
import timeit
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
file_log = current_dir + r"/logs/src.log"
# gerando log
logging.basicConfig(level=logging.DEBUG, filename=file_log, format="%(asctime)s - %(levelname)s - %(message)s")

def descompactador(diretorioatual:str = os.getcwd() , diretoriodestino:str =  os.getcwd()):
    """
        Buscamos no diretório informado os arquivos zip e faz a descompactação do mesmo no diretório também informado

        Parâmetros:
            diretorioatual : onde estaremos fazendo a busca
            diretoriodestino : para onde estaremos descompactando os arquivos
        
        Padrão
            os.getcwd() : Por padrão buscamos e retornamos o diretório atual onde você está rodando o script (Massa né)

        Libs utilizadas:
            OS : replica as funcionalidades do Sistema Operacional com Python (Massa também né?)
            zipfile.ZipFile : Como o nome diz replica um WinRaz da vida e faz toda a mágica por trás


    """
    from zipfile import ZipFile, BadZipFile, LargeZipFile
    os.system('cls')
    
    #diretorioatual = os.getcwd()
    #diretoriodestino = f'{diretorioatual}/ESTABELECIMENTOSCSV/'
    
    all_files = list(filter(lambda x: '.zip' in x, os.listdir(f'{diretorioatual}/')))
    for file in all_files:
        dirfile = os.path.abspath(f'{diretorioatual}/{file}')
        try:
            with ZipFile(dirfile,'r') as zlist:
                zlist.extractall(path=f'{diretoriodestino}/')
                zlist.close()
        except BadZipFile:
            logging.info(f'Não dá pra descompactar o arquivo {file}, está corrompido.')
            continue
        except LargeZipFile:
            logging.info(f'Não é possível descompactar o arquivo {file}, é grande demais.')
            continue
        except:
            logging.info(f'Não deu pra baixar o arquivo {file} por algum motivo')

        """for i in all_files:
            logging.info(os.path.abspath(i))"""
    tempo_gasto = timeit.timeit(stmt='a=10;b=10;sum=a+b')
    logging.info(f"Processo de conversão concluído, tempo gasto: {tempo_gasto}")
        