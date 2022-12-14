#python -m pydoc -w aws_athena_teste wrote aws_athena_teste.html
# definindo libs
import boto3 
import json
import time

# Parametros de conexão
CLIENT = boto3.client("athena")
DATA_CATALOG = "AwsDataCatalog"
DATABASE_NAME = "kukac"
RESULT_OUTPUT_LOCATION = "s3://abrasel-datalake/query-results/"
TABLE_NAME = "d_regiao"

# Verifica status consulta
def verifica_status_consulta(id_execucao:str):
    """Verifica a o status da consulta, e retorna True ou False para concluido"""

    status = "RUNNING"
    execucao_maxima = 5

    while execucao_maxima > 0 and status in ["RUNNING", "QUEUED"]:
        execucao_maxima -= 1
        resposta = CLIENT.get_query_execution(QueryExecutionId=id_execucao)
        if (
            "QueryExecution" in resposta
            and "Status" in resposta["QueryExecution"]
            and "State" in resposta["QueryExecution"]["Status"]
        ):
            status = resposta["QueryExecution"]["Status"]["State"]
            if status == "SUCCEEDED":
                return True

        time.sleep(15)

    return False



# Função para inserir a consulta no Athena
def consulta(query:str = f"""SELECT * FROM {DATA_CATALOG}.{DATABASE_NAME}.{TABLE_NAME} LIMIT 5"""):
    """ Execulta a consulta no Athena e retorna o Id da consulta para vizualizar com a função de visualização """
    resposta = CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
    )

    return resposta["QueryExecutionId"]
  


def mostra_dados_consulta(id_execucao:str):
    """ Retorna uma lista de dicionários com o resultado da consulta """
    resposta = CLIENT.get_query_results(
        QueryExecutionId=id_execucao
    )

    dict_dados = resposta['ResultSet']['Rows']
    return dict_dados