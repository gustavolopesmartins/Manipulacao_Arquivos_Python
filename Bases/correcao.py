import os
import pandas as pd

files = list(filter(lambda x: '.csv' in x, os.listdir("Bases/")))

for file in files:
    dados = pd.read_csv(f'Bases/{file}',sep=';')
    dados = dados.drop_duplicates()
    dados.to_csv(f'Bases/{file}', mode='w')