import os
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
    from zipfile import ZipFile
    os.system('cls')
    
    #diretorioatual = os.getcwd()
    #diretoriodestino = f'{diretorioatual}/ESTABELECIMENTOSCSV/'
    
    all_files = list(filter(lambda x: '.zip' in x, os.listdir(f'{diretorioatual}/')))
    for file in all_files:
        dirfile = os.path.abspath(file)
        with ZipFile(dirfile,'r') as zlist:
            zlist.extractall(path=f'{diretoriodestino}/')
        """for i in all_files:
            print(os.path.abspath(i))"""
    return 'Processo de descompactação concluído'
        