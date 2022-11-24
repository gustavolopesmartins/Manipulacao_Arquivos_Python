import os 
def movearquivos(diretorioatual:str = os.getcwd() , diretoriodestino:str =  os.getcwd(), extensao:str = '.' ):
    """
        Busca todos os arquivos com uma extenção específica e move de diretório

        Parâmetros :
            diretorioatual : Informe o caminho do diretório atual onde se encontra os arquivos 
            diretoriodestino : Informe o caminho do diretório destino que receberá os arquivos
            extensao : Opcional extenção dos arquivos
        
        Padrão
            os.getcwd() : Por padrão buscamos e retornamos o diretório atual onde você está rodando o script (Massa né)
            . : Por padrão trazemos todos os tipos de arquivos

        Libs utilizadas:
            OS : replica as funcionalidades do Sistema Operacional com Python (Massa também né?)
    """
    os.system('cls')
    #diretorioatual = f'{os.getcwd()}/'
    #diretoriodestino = f'{diretorioatual}ESTABELECIMENTOSZIP/'
    #extensao = '.zip'
    files = list(filter(lambda x: extensao in x, os.listdir(diretorioatual)))

    for file in files:
        #print(file)
        os.replace(f'{diretorioatual}{file}',f'{diretoriodestino}{file}')
    
    return 'Processo concluído'